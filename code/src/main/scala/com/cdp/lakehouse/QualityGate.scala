package com.cdp.lakehouse

import java.time.{Duration, Instant}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Data Trustworthiness & Quality Gate Module.
 *
 * Enforces contract invariants before promoting data from Silver to Gold:
 * 1. Completeness & Primary Key uniqueness.
 * 2. Strict financial money typing: integer paise (BIGINT, >= 0, no floats/nulls).
 * 3. Referential integrity: customer_id presence.
 * 4. Independent freshness SLA verification (source watermark vs clock).
 * 5. Source-to-lake control total reconciliation.
 */
object QualityGate {

  class QualityGateException(message: String) extends RuntimeException(message)

  final case class QualityResult(
      rowCount: Long,
      duplicatePaymentIds: Long,
      invalidMoneyRows: Long,
      missingCustomerIds: Long,
      contractViolations: Seq[String] = Seq.empty
  ) {
    def passed: Boolean =
      rowCount > 0 &&
      duplicatePaymentIds == 0 &&
      invalidMoneyRows == 0 &&
      missingCustomerIds == 0 &&
      contractViolations.isEmpty
  }

  /**
   * Validates schema contract against required columns.
   */
  def checkContract(df: DataFrame, requiredColumns: Seq[String]): Seq[String] = {
    val existing = df.columns.toSet
    val missing = requiredColumns.filterNot(existing.contains)
    if (missing.nonEmpty) {
      Seq(s"Missing required schema columns: ${missing.mkString(", ")}")
    } else {
      Seq.empty
    }
  }

  /**
   * Runs the full payment quality gate suite against a Spark DataFrame in a SINGLE PASS.
   *
   * Avoids multiple table scans and standalone shuffles on 500M+ row batches by computing
   * row count, primary key uniqueness, money invariants, and referential integrity
   * within a single conditional aggregation query.
   */
  def validatePayments(df: DataFrame, minRows: Long = 1L): QualityResult = {
    val contractErrors = checkContract(df, Seq("payment_id", "customer_id", "amount_paise"))

    // Fast contract check: if required columns are missing, abort before scanning
    if (contractErrors.nonEmpty) {
      return QualityResult(
        rowCount = 0L,
        duplicatePaymentIds = 0L,
        invalidMoneyRows = 0L,
        missingCustomerIds = 0L,
        contractViolations = contractErrors
      )
    }

    // Single-pass aggregation computing all invariant metrics
    val metricsRow = df.select(
      count(lit(1)).as("total_rows"),
      count(col("payment_id")).as("non_null_pids"),
      countDistinct(col("payment_id")).as("distinct_pids"),
      sum(when(col("payment_id").isNull, 1L).otherwise(0L)).as("null_pids"),
      sum(
        when(
          col("amount_paise").isNull ||
          col("amount_paise") < 0 ||
          col("amount_paise").cast("long").isNull ||
          col("amount_paise") =!= col("amount_paise").cast("long") ||
          isnan(col("amount_paise")),
          1L
        ).otherwise(0L)
      ).as("invalid_money_rows"),
      sum(
        when(
          col("customer_id").isNull || trim(col("customer_id").cast("string")) === "",
          1L
        ).otherwise(0L)
      ).as("missing_customer_ids")
    ).head()

    val totalRows = metricsRow.getAs[Long]("total_rows")
    val nonNullPids = metricsRow.getAs[Long]("non_null_pids")
    val distinctPids = metricsRow.getAs[Long]("distinct_pids")
    val nullPids = metricsRow.getAs[Long]("null_pids")
    val invalidMoneyRows = metricsRow.getAs[Long]("invalid_money_rows")
    val missingCustomerIds = metricsRow.getAs[Long]("missing_customer_ids")

    // Excess duplicate records plus any null PKs
    val duplicatePaymentViolations = (nonNullPids - distinctPids) + nullPids

    val violations = if (totalRows < minRows) {
      contractErrors :+ s"Row count ($totalRows) below required minimum ($minRows)"
    } else {
      contractErrors
    }

    QualityResult(
      rowCount = totalRows,
      duplicatePaymentIds = duplicatePaymentViolations,
      invalidMoneyRows = invalidMoneyRows,
      missingCustomerIds = missingCustomerIds,
      contractViolations = violations
    )
  }

  /**
   * Blocking quality gate. Throws exception and prevents downstream promotion if invalid.
   */
  def requireValidPayments(df: DataFrame): QualityResult = {
    val result = validatePayments(df)
    if (!result.passed) {
      throw new QualityGateException(
        s"Payment quality gate failed! Summary: " +
          s"rowCount=${result.rowCount}, " +
          s"duplicatePaymentIds=${result.duplicatePaymentIds}, " +
          s"invalidMoneyRows=${result.invalidMoneyRows}, " +
          s"missingCustomerIds=${result.missingCustomerIds}, " +
          s"contractViolations=[${result.contractViolations.mkString("; ")}]"
      )
    }
    result
  }

  /**
   * Computes data freshness lag in minutes between source watermark and reference timestamp.
   */
  def freshnessMinutes(sourceWatermark: Instant, now: Instant = Instant.now()): Double = {
    Duration.between(sourceWatermark, now).toSeconds.toDouble / 60.0
  }

  /**
   * Verifies if data meets freshness SLA threshold.
   */
  def isFresh(sourceWatermark: Instant, maxLagMinutes: Double, now: Instant = Instant.now()): Boolean = {
    freshnessMinutes(sourceWatermark, now) <= maxLagMinutes
  }

  /**
   * Reconciles source control totals with lakehouse record counts within an allowed tolerance.
   */
  def reconcileCounts(sourceCount: Long, lakeCount: Long, tolerance: Long = 0L): Boolean = {
    Math.abs(sourceCount - lakeCount) <= tolerance
  }
}

