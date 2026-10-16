package com.cdp.lakehouse

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LakehouseHardPartsSpec extends AnyFunSuite with Matchers {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("CDPLakehouseHardPartsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  import spark.implicits._

  // =========================================================================
  // 1. Replay-Safe Ingestion Tests
  // =========================================================================

  test("deterministic event IDs remain identical across retries with identical payload") {
    val input = Seq(
      ("pay_1001", "cust_201", 50000L),
      ("pay_1002", "cust_202", 12500L)
    ).toDF("source_id", "customer_id", "amount_paise")

    val bronze1 = Ingestion.normalizeBronze(input, "payments_service", "transaction")
    val bronze2 = Ingestion.normalizeBronze(input, "payments_service", "transaction")

    val ids1 = bronze1.select("_event_id").as[String].collect()
    val ids2 = bronze2.select("_event_id").as[String].collect()

    ids1 should contain theSameElementsInOrderAs ids2
  }

  test("canonical sorted JSON keys guarantee identical event ID regardless of upstream column ordering") {
    // Two DataFrames with identical data but columns in opposite order
    val dfA = Seq(("u1", "Delhi", 500L)).toDF("user_id", "city", "amount")
    val dfB = Seq(("Delhi", 500L, "u1")).toDF("city", "amount", "user_id")

    val bronzeA = Ingestion.normalizeBronze(dfA, "app_events", "login")
    val bronzeB = Ingestion.normalizeBronze(dfB, "app_events", "login")

    val idA = bronzeA.select("_event_id").as[String].head()
    val idB = bronzeB.select("_event_id").as[String].head()

    idA should equal(idB)
  }

  test("CdcMerge rejects invalid or injected table identifiers or composite keys") {
    intercept[IllegalArgumentException] {
      CdcMerge.buildMergeSql("silver_payments; DROP TABLE gold_ledger;--", "valid_view")
    }
    intercept[IllegalArgumentException] {
      CdcMerge.buildMergeSql("silver_payments", "invalid view with spaces")
    }
    intercept[IllegalArgumentException] {
      CdcMerge.buildMergeSql("silver_payments", "valid_view", Seq("key; DROP TABLE gold_ledger;--"))
    }
  }

  test("CdcMerge generates valid composite primary key ON condition") {
    val sql = CdcMerge.buildMergeSql("cdp_catalog.silver.payments", "incoming_cdc", Seq("tenant_id", "payment_id"))
    sql should include("ON t.tenant_id = s.tenant_id AND t.payment_id = s.payment_id")
    sql should include("INSERT (tenant_id, payment_id, value, deleted, transaction_id, sequence_id, updated_at)")
  }

  test("fallback payload hashing creates deterministic ID when source_id is absent or blank") {
    val id1 = Ingestion.computeEventId(
      source = "partner_stream",
      entity = "click",
      sourceId = None,
      fallbackPayload = """{"user_id":"u1","page":"checkout"}"""
    )
    val id2 = Ingestion.computeEventId(
      source = "partner_stream",
      entity = "click",
      sourceId = Some("   "), // Blank whitespace treated as absent
      fallbackPayload = """{"user_id":"u1","page":"checkout"}"""
    )
    val id3 = Ingestion.computeEventId(
      source = "partner_stream",
      entity = "click",
      sourceId = None,
      fallbackPayload = """{"user_id":"u1","page":"home"}"""
    )

    id1 should equal(id2)
    id1 should not equal(id3)
  }

  test("file manifest registry correctly handles new, duplicate, and conflicting deliveries") {
    val registry = new Ingestion.FileManifestRegistry()
    val manifestA = Ingestion.FileManifest(
      source = "partner_merchant_settlement",
      deliveryId = "batch_delivery_01",
      uri = "s3://lakehouse-landing/partner/batch_01.csv",
      sizeBytes = 1048576L,
      sha256Checksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )

    // First delivery must be registered as NEW
    registry.register(manifestA) should equal(Ingestion.NewDelivery)

    // Exact replay must be flagged as DUPLICATE (idempotent no-op)
    registry.register(manifestA) should equal(Ingestion.DuplicateDelivery)

    // Delivery with modified content for same deliveryId must be REJECTED with conflict
    val tamperedManifest = manifestA.copy(sizeBytes = 1048600L)
    intercept[Ingestion.DeliveryConflictException] {
      registry.register(tamperedManifest)
    }
  }

  // =========================================================================
  // 2. CDC Correctness & State Reconstruction Tests
  // =========================================================================

  test("out-of-order CDC updates do not overwrite newer target state") {
    import CdcMerge._

    val initial: Map[String, CurrentRow] = Map.empty

    // Event 2 arrived first: LSN 100, Seq 2 -> status = "COMPLETED"
    val newerEvent = CDCEvent(
      key = "order_99",
      position = SourcePosition(100L, 2L),
      operation = Operation.Update,
      value = Some("""{"status":"COMPLETED"}""")
    )

    val stateAfterNewer = applyCdc(initial, Seq(newerEvent))
    stateAfterNewer("order_99").position should equal(SourcePosition(100L, 2L))
    stateAfterNewer("order_99").value should equal(Some("""{"status":"COMPLETED"}"""))

    // Late Event 1 arrived after: LSN 100, Seq 1 -> status = "INITIATED"
    val staleEvent = CDCEvent(
      key = "order_99",
      position = SourcePosition(100L, 1L),
      operation = Operation.Update,
      value = Some("""{"status":"INITIATED"}""")
    )

    val stateAfterStale = applyCdc(stateAfterNewer, Seq(staleEvent))
    // State MUST NOT regress to "INITIATED"
    stateAfterStale("order_99").position should equal(SourcePosition(100L, 2L))
    stateAfterStale("order_99").value should equal(Some("""{"status":"COMPLETED"}"""))
  }

  test("CDC delete records a soft-delete tombstone with correct source position") {
    import CdcMerge._

    val state = Map(
      "user_42" -> CurrentRow(
        key = "user_42",
        position = SourcePosition(50L, 1L),
        deleted = false,
        value = Some("""{"kyc":"VERIFIED"}""")
      )
    )

    val deleteEvent = CDCEvent(
      key = "user_42",
      position = SourcePosition(50L, 2L),
      operation = Operation.Delete,
      value = None
    )

    val updatedState = applyCdc(state, Seq(deleteEvent))
    updatedState("user_42").deleted should be(true)
    updatedState("user_42").value should be(None)
    updatedState("user_42").position should equal(SourcePosition(50L, 2L))
  }

  test("CDC microbatch deduplication in Spark picks highest (transaction_id, sequence_id)") {
    val batch = Seq(
      ("k1", """{"v":1}""", "U", 10L, 1L),
      ("k1", """{"v":3}""", "U", 10L, 3L),
      ("k1", """{"v":2}""", "U", 10L, 2L),
      ("k2", """{"v":10}""", "I", 5L, 1L)
    ).toDF("key", "value", "operation", "transaction_id", "sequence_id")

    val deduplicated = CdcMerge.deduplicateMicroBatch(batch)
    val results = deduplicated
      .select("key", "value", "sequence_id")
      .as[(String, String, Long)]
      .collect()
      .map { case (k, v, s) => k -> (v, s) }
      .toMap

    results("k1") should equal(("""{"v":3}""", 3L))
    results("k2") should equal(("""{"v":10}""", 1L))
  }

  // =========================================================================
  // 3. Quality Gate & Trustworthiness Tests
  // =========================================================================

  test("quality gate rejects duplicate payment_ids and invalid integer paise") {
    val invalidBatch = Seq(
      ("pay_1", "cust_1", 1000L),
      ("pay_1", "cust_2", 2000L),  // duplicate payment_id
      ("pay_2", "cust_3", -50L),   // negative amount_paise
      ("pay_3", null,     3000L)   // missing customer_id
    ).toDF("payment_id", "customer_id", "amount_paise")

    val result = QualityGate.validatePayments(invalidBatch)
    result.passed should be(false)
    result.duplicatePaymentIds should be(1L)
    result.invalidMoneyRows should be(1L)
    result.missingCustomerIds should be(1L)

    intercept[QualityGate.QualityGateException] {
      QualityGate.requireValidPayments(invalidBatch)
    }
  }

  test("CDC microbatch deduplication filters out records with null primary keys") {
    val batch = Seq(
      ("k1", """{"v":1}""", "I", 10L, 1L),
      (null.asInstanceOf[String], """{"v":2}""", "I", 10L, 2L)
    ).toDF("key", "value", "operation", "transaction_id", "sequence_id")

    val deduplicated = CdcMerge.deduplicateMicroBatch(batch)
    val keys = deduplicated.select("key").as[String].collect()
    keys should contain only ("k1")
  }

  test("quality gate accepts clean dataset with unique keys and positive paise") {
    val validBatch = Seq(
      ("pay_1", "cust_1", 10000L),
      ("pay_2", "cust_2", 25000L),
      ("pay_3", "cust_3", 500000L)
    ).toDF("payment_id", "customer_id", "amount_paise")

    val result = QualityGate.requireValidPayments(validBatch)
    result.passed should be(true)
    result.rowCount should be(3L)
    result.duplicatePaymentIds should be(0L)
    result.invalidMoneyRows should be(0L)
    result.missingCustomerIds should be(0L)
  }

  test("freshness SLA calculation correctly detects stale data") {
    val now = Instant.now()
    val freshWatermark = now.minus(5, ChronoUnit.MINUTES)
    val staleWatermark = now.minus(45, ChronoUnit.MINUTES)

    QualityGate.isFresh(freshWatermark, maxLagMinutes = 15.0, now = now) should be(true)
    QualityGate.isFresh(staleWatermark, maxLagMinutes = 15.0, now = now) should be(false)
  }

  test("reconcileCounts allows tolerance threshold") {
    QualityGate.reconcileCounts(sourceCount = 10000L, lakeCount = 10000L, tolerance = 0L) should be(true)
    QualityGate.reconcileCounts(sourceCount = 10000L, lakeCount = 9998L, tolerance = 5L) should be(true)
    QualityGate.reconcileCounts(sourceCount = 10000L, lakeCount = 9990L, tolerance = 5L) should be(false)
  }
}

