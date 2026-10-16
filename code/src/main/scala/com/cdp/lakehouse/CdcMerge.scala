package com.cdp.lakehouse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * CDC Correctness & State Reconstruction Module.
 *
 * Guarantees:
 * 1. Source-position ordering (transaction ID + sequence ID) prevents stale event overwrites.
 * 2. In-microbatch deduplication handles redundant duplicate messages before MERGE.
 * 3. Soft-delete tombstones are preserved for auditability and point-in-time correctness.
 * 4. Full idempotency on pipeline retries.
 */
object CdcMerge {

  sealed trait Operation {
    def code: String
  }
  object Operation {
    case object Insert extends Operation { val code = "I" }
    case object Update extends Operation { val code = "U" }
    case object Delete extends Operation { val code = "D" }

    def fromCode(code: String): Operation = code.trim.toUpperCase match {
      case "I" => Insert
      case "U" => Update
      case "D" => Delete
      case other => throw new IllegalArgumentException(s"Unsupported CDC operation code: $other")
    }
  }

  final case class SourcePosition(transactionId: Long, sequenceId: Long)
      extends Ordered[SourcePosition] {
    override def compare(that: SourcePosition): Int = {
      val txCmp = this.transactionId.compare(that.transactionId)
      if (txCmp != 0) txCmp else this.sequenceId.compare(that.sequenceId)
    }
  }

  final case class CDCEvent(
      key: String,
      position: SourcePosition,
      operation: Operation,
      value: Option[String]
  )

  final case class CurrentRow(
      key: String,
      position: SourcePosition,
      deleted: Boolean,
      value: Option[String]
  )

  /**
   * Pure functional reference state engine.
   * Models the exact semantics of Iceberg table state transitions across CDC events.
   * Enables hermetic unit testing without requiring an active Iceberg cluster.
   */
  def applyCdc(
      state: Map[String, CurrentRow],
      events: Seq[CDCEvent]
  ): Map[String, CurrentRow] = {
    var result = state

    // Order incoming events deterministically by key, then monotonically by position
    val sortedEvents = events.sortBy(e => (e.key, e.position))

    for (event <- sortedEvents) {
      val current = result.get(event.key)
      val isStale = current.exists(curr => event.position <= curr.position)

      if (!isStale) {
        event.operation match {
          case Operation.Delete =>
            result = result.updated(
              event.key,
              CurrentRow(
                key = event.key,
                position = event.position,
                deleted = true,
                value = None
              )
            )
          case Operation.Insert | Operation.Update =>
            require(event.value.isDefined, s"Operation ${event.operation} requires non-null value")
            result = result.updated(
              event.key,
              CurrentRow(
                key = event.key,
                position = event.position,
                deleted = false,
                value = event.value
              )
            )
        }
      }
    }

    result
  }

  /**
   * Deduplicates within a micro-batch DataFrame so that only the highest
   * (transaction_id, sequence_id) per primary key is sent to the MERGE statement.
   * Corrupted records with NULL primary keys are filtered out to prevent poisoning.
   */
  def deduplicateMicroBatch(
      events: DataFrame,
      primaryKeys: Seq[String] = Seq("key")
  ): DataFrame = {
    require(primaryKeys.nonEmpty, "At least one primary key column must be specified")
    val nonNullCondition = primaryKeys.map(k => col(k).isNotNull).reduce(_ and _)
    val pkCols = primaryKeys.map(col)
    val window = Window
      .partitionBy(pkCols: _*)
      .orderBy(col("transaction_id").desc, col("sequence_id").desc)

    events
      .filter(nonNullCondition)
      .withColumn("_rn", row_number().over(window))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }

  /**
   * Generates the SQL statement for atomic Apache Iceberg MERGE INTO.
   *
   * Supports single or composite primary keys with strict regex validation
   * to guarantee injection-safe catalog queries.
   */
  def buildMergeSql(
      targetTable: String,
      sourceTempView: String,
      primaryKeys: Seq[String] = Seq("key")
  ): String = {
    require(
      targetTable.matches("^[a-zA-Z0-9_.]+$"),
      s"Invalid Iceberg target table identifier: '$targetTable'. Must match [a-zA-Z0-9_.]+"
    )
    require(
      sourceTempView.matches("^[a-zA-Z0-9_]+$"),
      s"Invalid source temporary view identifier: '$sourceTempView'"
    )
    require(
      primaryKeys.nonEmpty && primaryKeys.forall(_.matches("^[a-zA-Z0-9_]+$")),
      s"Invalid primary keys: ${primaryKeys.mkString(", ")}. Each must match [a-zA-Z0-9_]+"
    )

    val joinCondition = primaryKeys.map(k => s"t.$k = s.$k").mkString(" AND ")
    val insertColumns = (primaryKeys ++ Seq("value", "deleted", "transaction_id", "sequence_id", "updated_at")).mkString(", ")
    val insertValuesActive = (primaryKeys.map(k => s"s.$k") ++ Seq("s.value", "false", "s.transaction_id", "s.sequence_id", "current_timestamp()")).mkString(", ")
    val insertValuesDelete = (primaryKeys.map(k => s"s.$k") ++ Seq("CAST(NULL AS STRING)", "true", "s.transaction_id", "s.sequence_id", "current_timestamp()")).mkString(", ")

    s"""
       |MERGE INTO $targetTable AS t
       |USING $sourceTempView AS s
       |ON $joinCondition
       |WHEN MATCHED AND
       |  (s.transaction_id > t.transaction_id OR
       |   (s.transaction_id = t.transaction_id AND
       |    s.sequence_id > t.sequence_id))
       |  AND s.operation = 'D'
       |THEN UPDATE SET
       |  t.deleted = true,
       |  t.value = CAST(NULL AS STRING),
       |  t.transaction_id = s.transaction_id,
       |  t.sequence_id = s.sequence_id,
       |  t.updated_at = current_timestamp()
       |WHEN MATCHED AND
       |  (s.transaction_id > t.transaction_id OR
       |   (s.transaction_id = t.transaction_id AND
       |    s.sequence_id > t.sequence_id))
       |  AND s.operation IN ('I', 'U')
       |THEN UPDATE SET
       |  t.deleted = false,
       |  t.value = s.value,
       |  t.transaction_id = s.transaction_id,
       |  t.sequence_id = s.sequence_id,
       |  t.updated_at = current_timestamp()
       |WHEN NOT MATCHED AND s.operation IN ('I', 'U')
       |THEN INSERT ($insertColumns)
       |     VALUES ($insertValuesActive)
       |WHEN NOT MATCHED AND s.operation = 'D'
       |THEN INSERT ($insertColumns)
       |     VALUES ($insertValuesDelete)
       |""".stripMargin
  }

  /**
   * Applies incoming CDC events to an Apache Iceberg table idempotently.
   *
   * Iceberg automatically handles concurrent writes via optimistic concurrency retries,
   * while the monotonic SQL predicate ensures that retried commits never alter newer table state.
   */
  def mergeToIceberg(
      spark: SparkSession,
      events: DataFrame,
      targetTable: String,
      primaryKeys: Seq[String] = Seq("key")
  ): Unit = {
    val latestPerKey = deduplicateMicroBatch(events, primaryKeys)
    val tempViewName = s"incoming_cdc_${System.currentTimeMillis()}"
    latestPerKey.createOrReplaceTempView(tempViewName)

    try {
      spark.sql(buildMergeSql(targetTable, tempViewName, primaryKeys))
    } finally {
      spark.catalog.dropTempView(tempViewName)
    }
  }
}

