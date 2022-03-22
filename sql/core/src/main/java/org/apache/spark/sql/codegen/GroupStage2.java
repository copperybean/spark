package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

// == Subtree 2 / 2 (maxMethodCodeSize:523; maxConstantPoolSize:321(0.49% used); numInnerClasses:2) ==
// *(2) HashAggregate(keys=[ss_quantity#248], functions=[sum(ss_ticket_number#247L), avg(ss_ticket_number#247L), avg(UnscaledValue(ss_wholesale_cost#249))], output=[sum(ss_ticket_number)#345L, ss_quantity#248, avg(ss_ticket_number)#346, avg(ss_wholesale_cost)#347])
// +- Exchange hashpartitioning(ss_quantity#248, 1), ENSURE_REQUIREMENTS, [id=#327]
//    +- *(1) HashAggregate(keys=[ss_quantity#248], functions=[partial_sum(ss_ticket_number#247L), partial_avg(ss_ticket_number#247L), partial_avg(UnscaledValue(ss_wholesale_cost#249))], output=[ss_quantity#248, sum#353L, sum#354, count#355L, sum#356, count#357L])
//       +- *(1) Project [ss_ticket_number#247L, ss_quantity#248, ss_wholesale_cost#249]
//          +- *(1) ColumnarToRow
//             +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.store_sales[ss_ticket_number#247L,ss_quantity#248,ss_wholesale_cost#249,ss_sold_date_sk#261] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,..., PartitionFilters: [isnotnull(ss_sold_date_sk#261), (ss_sold_date_sk#261 = 2452640)], PushedFilters: [], ReadSchema: struct<ss_ticket_number:bigint,ss_quantity:int,ss_wholesale_cost:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage2(references);
// }

// codegenStageId=2
final class GroupStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg_0;
  private boolean agg_bufIsNull_0;
  private long agg_bufValue_0;
  private boolean agg_bufIsNull_1;
  private double agg_bufValue_1;
  private boolean agg_bufIsNull_2;
  private long agg_bufValue_2;
  private boolean agg_bufIsNull_3;
  private double agg_bufValue_3;
  private boolean agg_bufIsNull_4;
  private long agg_bufValue_4;
  private agg_FastHashMap_0 agg_fastHashMap_0;
  private org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> agg_fastHashMapIter_0;
  private org.apache.spark.unsafe.KVIterator agg_mapIter_0;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap_0;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter_0;
  private scala.collection.Iterator inputadapter_input_0;
  private boolean agg_agg_isNull_7_0;
  private boolean agg_agg_isNull_9_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] agg_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public GroupStage2(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    inputadapter_input_0 = inputs[0];
    agg_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    agg_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 0);

  }

  public class agg_FastHashMap_0 {
    private org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch batch;
    private int[] buckets;
    private int capacity = 1 << 16;
    private double loadFactor = 0.5;
    private int numBuckets = (int) (capacity / loadFactor);
    private int maxSteps = 2;
    private int numRows = 0;
    private Object emptyVBase;
    private long emptyVOff;
    private int emptyVLen;
    private boolean isBatchFull = false;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;

    public agg_FastHashMap_0(
      org.apache.spark.memory.TaskMemoryManager taskMemoryManager,
      InternalRow emptyAggregationBuffer) {
      batch = org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch
      .allocate(((org.apache.spark.sql.types.StructType) references[1] /* keySchemaTerm */), ((org.apache.spark.sql.types.StructType) references[2] /* valueSchemaTerm */), taskMemoryManager, capacity);

      final UnsafeProjection valueProjection = UnsafeProjection.create(((org.apache.spark.sql.types.StructType) references[2] /* valueSchemaTerm */));
      final byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();

      emptyVBase = emptyBuffer;
      emptyVOff = Platform.BYTE_ARRAY_OFFSET;
      emptyVLen = emptyBuffer.length;

      agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(
        1, 0);

      buckets = new int[numBuckets];
      java.util.Arrays.fill(buckets, -1);
    }

    public org.apache.spark.sql.catalyst.expressions.UnsafeRow findOrInsert(int agg_key_0) {
      long h = hash(agg_key_0);
      int step = 0;
      int idx = (int) h & (numBuckets - 1);
      while (step < maxSteps) {
        // Return bucket index if it's either an empty slot or already contains the key
        if (buckets[idx] == -1) {
          if (numRows < capacity && !isBatchFull) {
            agg_rowWriter.reset();
            agg_rowWriter.zeroOutNullBytes();
            agg_rowWriter.write(0, agg_key_0);
            org.apache.spark.sql.catalyst.expressions.UnsafeRow agg_result
            = agg_rowWriter.getRow();
            Object kbase = agg_result.getBaseObject();
            long koff = agg_result.getBaseOffset();
            int klen = agg_result.getSizeInBytes();

            UnsafeRow vRow
            = batch.appendRow(kbase, koff, klen, emptyVBase, emptyVOff, emptyVLen);
            if (vRow == null) {
              isBatchFull = true;
            } else {
              buckets[idx] = numRows++;
            }
            return vRow;
          } else {
            // No more space
            return null;
          }
        } else if (equals(idx, agg_key_0)) {
          return batch.getValueRow(buckets[idx]);
        }
        idx = (idx + 1) & (numBuckets - 1);
        step++;
      }
      // Didn't find it
      return null;
    }

    private boolean equals(int idx, int agg_key_0) {
      UnsafeRow row = batch.getKeyRow(buckets[idx]);
      return (row.getInt(0) == agg_key_0);
    }

    private long hash(int agg_key_0) {
      long agg_hash_0 = 0;

      int agg_result_0 = agg_key_0;
      agg_hash_0 = (agg_hash_0 ^ (0x9e3779b9)) + agg_result_0 + (agg_hash_0 << 6) + (agg_hash_0 >>> 2);

      return agg_hash_0;
    }

    public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
      return batch.rowIterator();
    }

    public void close() {
      batch.close();
    }

  }

  private void agg_doAggregate_sum_0(boolean agg_exprIsNull_1_0, long agg_expr_1_0, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0) throws java.io.IOException {
    agg_agg_isNull_7_0 = true;
    long agg_value_12 = -1L;
    do {
      boolean agg_isNull_8 = true;
      long agg_value_13 = -1L;
      agg_agg_isNull_9_0 = true;
      long agg_value_14 = -1L;
      do {
        boolean agg_isNull_10 = agg_unsafeRowAggBuffer_0.isNullAt(0);
        long agg_value_15 = agg_isNull_10 ?
        -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
        if (!agg_isNull_10) {
          agg_agg_isNull_9_0 = false;
          agg_value_14 = agg_value_15;
          continue;
        }

        if (!false) {
          agg_agg_isNull_9_0 = false;
          agg_value_14 = 0L;
          continue;
        }

      } while (false);

      if (!agg_exprIsNull_1_0) {
        agg_isNull_8 = false; // resultCode could change nullability.

        agg_value_13 = agg_value_14 + agg_expr_1_0;

      }
      if (!agg_isNull_8) {
        agg_agg_isNull_7_0 = false;
        agg_value_12 = agg_value_13;
        continue;
      }

      boolean agg_isNull_13 = agg_unsafeRowAggBuffer_0.isNullAt(0);
      long agg_value_18 = agg_isNull_13 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
      if (!agg_isNull_13) {
        agg_agg_isNull_7_0 = false;
        agg_value_12 = agg_value_18;
        continue;
      }

    } while (false);

    if (!agg_agg_isNull_7_0) {
      agg_unsafeRowAggBuffer_0.setLong(0, agg_value_12);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(0);
    }
  }

  private void agg_doAggregateWithKeysOutput_0(UnsafeRow agg_keyTerm_0, UnsafeRow agg_bufferTerm_0)
  throws java.io.IOException {
    ((org.apache.spark.sql.execution.metric.SQLMetric) references[7] /* numOutputRows */).add(1);

    boolean agg_isNull_26 = agg_keyTerm_0.isNullAt(0);
    int agg_value_31 = agg_isNull_26 ?
    -1 : (agg_keyTerm_0.getInt(0));
    boolean agg_isNull_27 = agg_bufferTerm_0.isNullAt(0);
    long agg_value_32 = agg_isNull_27 ?
    -1L : (agg_bufferTerm_0.getLong(0));
    boolean agg_isNull_28 = agg_bufferTerm_0.isNullAt(1);
    double agg_value_33 = agg_isNull_28 ?
    -1.0 : (agg_bufferTerm_0.getDouble(1));
    boolean agg_isNull_29 = agg_bufferTerm_0.isNullAt(2);
    long agg_value_34 = agg_isNull_29 ?
    -1L : (agg_bufferTerm_0.getLong(2));
    boolean agg_isNull_30 = agg_bufferTerm_0.isNullAt(3);
    double agg_value_35 = agg_isNull_30 ?
    -1.0 : (agg_bufferTerm_0.getDouble(3));
    boolean agg_isNull_31 = agg_bufferTerm_0.isNullAt(4);
    long agg_value_36 = agg_isNull_31 ?
    -1L : (agg_bufferTerm_0.getLong(4));
    boolean agg_isNull_35 = agg_isNull_29;
    double agg_value_40 = -1.0;
    if (!agg_isNull_29) {
      agg_value_40 = (double) agg_value_34;
    }
    boolean agg_isNull_33 = false;
    double agg_value_38 = -1.0;
    if (agg_isNull_35 || agg_value_40 == 0) {
      agg_isNull_33 = true;
    } else {
      if (agg_isNull_28) {
        agg_isNull_33 = true;
      } else {
        agg_value_38 = (double)(agg_value_33 / agg_value_40);
      }
    }
    boolean agg_isNull_39 = agg_isNull_31;
    double agg_value_44 = -1.0;
    if (!agg_isNull_31) {
      agg_value_44 = (double) agg_value_36;
    }
    boolean agg_isNull_37 = false;
    double agg_value_42 = -1.0;
    if (agg_isNull_39 || agg_value_44 == 0) {
      agg_isNull_37 = true;
    } else {
      if (agg_isNull_30) {
        agg_isNull_37 = true;
      } else {
        agg_value_42 = (double)(agg_value_35 / agg_value_44);
      }
    }

    boolean agg_isNull_45 = false;
    double agg_value_50 = -1.0;
    if (false || 100.0D == 0) {
      agg_isNull_45 = true;
    } else {
      if (agg_isNull_37) {
        agg_isNull_45 = true;
      } else {
        agg_value_50 = (double)(agg_value_42 / 100.0D);
      }
    }
    boolean agg_isNull_44 = agg_isNull_45;
    Decimal agg_value_49 = null;
    if (!agg_isNull_45) {
      try {
        Decimal agg_tmpDecimal_0 = Decimal.apply(scala.math.BigDecimal.valueOf((double) agg_value_50));
        if (agg_tmpDecimal_0.changePrecision(11, 6)) {
          agg_value_49 = agg_tmpDecimal_0;
        } else {
          agg_isNull_44 = true;
        }
      } catch (java.lang.NumberFormatException e) {
        agg_isNull_44 = true;
      }
    }
    agg_mutableStateArray_0[1].reset();

    agg_mutableStateArray_0[1].zeroOutNullBytes();

    if (agg_isNull_27) {
      agg_mutableStateArray_0[1].setNullAt(0);
    } else {
      agg_mutableStateArray_0[1].write(0, agg_value_32);
    }

    if (agg_isNull_26) {
      agg_mutableStateArray_0[1].setNullAt(1);
    } else {
      agg_mutableStateArray_0[1].write(1, agg_value_31);
    }

    if (agg_isNull_33) {
      agg_mutableStateArray_0[1].setNullAt(2);
    } else {
      agg_mutableStateArray_0[1].write(2, agg_value_38);
    }

    if (agg_isNull_44) {
      agg_mutableStateArray_0[1].setNullAt(3);
    } else {
      agg_mutableStateArray_0[1].write(3, agg_value_49, 11, 6);
    }
    append((agg_mutableStateArray_0[1].getRow()));

  }

  private void agg_doAggregate_avg_1(boolean agg_exprIsNull_4_0, boolean agg_exprIsNull_5_0, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0, double agg_expr_4_0, long agg_expr_5_0) throws java.io.IOException {
    boolean agg_isNull_20 = true;
    double agg_value_25 = -1.0;
    boolean agg_isNull_21 = agg_unsafeRowAggBuffer_0.isNullAt(3);
    double agg_value_26 = agg_isNull_21 ?
    -1.0 : (agg_unsafeRowAggBuffer_0.getDouble(3));
    if (!agg_isNull_21) {
      if (!agg_exprIsNull_4_0) {
        agg_isNull_20 = false; // resultCode could change nullability.

        agg_value_25 = agg_value_26 + agg_expr_4_0;

      }

    }
    boolean agg_isNull_23 = true;
    long agg_value_28 = -1L;
    boolean agg_isNull_24 = agg_unsafeRowAggBuffer_0.isNullAt(4);
    long agg_value_29 = agg_isNull_24 ?
    -1L : (agg_unsafeRowAggBuffer_0.getLong(4));
    if (!agg_isNull_24) {
      if (!agg_exprIsNull_5_0) {
        agg_isNull_23 = false; // resultCode could change nullability.

        agg_value_28 = agg_value_29 + agg_expr_5_0;

      }

    }

    if (!agg_isNull_20) {
      agg_unsafeRowAggBuffer_0.setDouble(3, agg_value_25);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(3);
    }

    if (!agg_isNull_23) {
      agg_unsafeRowAggBuffer_0.setLong(4, agg_value_28);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(4);
    }
  }

  private void agg_doConsume_0(InternalRow inputadapter_row_0, int agg_expr_0_0, boolean agg_exprIsNull_0_0, long agg_expr_1_0, boolean agg_exprIsNull_1_0, double agg_expr_2_0, boolean agg_exprIsNull_2_0, long agg_expr_3_0, boolean agg_exprIsNull_3_0, double agg_expr_4_0, boolean agg_exprIsNull_4_0, long agg_expr_5_0, boolean agg_exprIsNull_5_0) throws java.io.IOException {
    UnsafeRow agg_unsafeRowAggBuffer_0 = null;
    UnsafeRow agg_fastAggBuffer_0 = null;

    if (!agg_exprIsNull_0_0) {
      agg_fastAggBuffer_0 = agg_fastHashMap_0.findOrInsert(
        agg_expr_0_0);
    }
    // Cannot find the key in fast hash map, try regular hash map.
    if (agg_fastAggBuffer_0 == null) {
      // generate grouping key
      agg_mutableStateArray_0[0].reset();

      agg_mutableStateArray_0[0].zeroOutNullBytes();

      if (agg_exprIsNull_0_0) {
        agg_mutableStateArray_0[0].setNullAt(0);
      } else {
        agg_mutableStateArray_0[0].write(0, agg_expr_0_0);
      }
      int agg_unsafeRowKeyHash_0 = (agg_mutableStateArray_0[0].getRow()).hashCode();
      if (true) {
        // try to get the buffer from hash map
        agg_unsafeRowAggBuffer_0 =
        agg_hashMap_0.getAggregationBufferFromUnsafeRow((agg_mutableStateArray_0[0].getRow()), agg_unsafeRowKeyHash_0);
      }
      // Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
      // aggregation after processing all input rows.
      if (agg_unsafeRowAggBuffer_0 == null) {
        if (agg_sorter_0 == null) {
          agg_sorter_0 = agg_hashMap_0.destructAndCreateExternalSorter();
        } else {
          agg_sorter_0.merge(agg_hashMap_0.destructAndCreateExternalSorter());
        }

        // the hash map had be spilled, it should have enough memory now,
        // try to allocate buffer again.
        agg_unsafeRowAggBuffer_0 = agg_hashMap_0.getAggregationBufferFromUnsafeRow(
          (agg_mutableStateArray_0[0].getRow()), agg_unsafeRowKeyHash_0);
        if (agg_unsafeRowAggBuffer_0 == null) {
          // failed to allocate the first page
          throw new org.apache.spark.memory.SparkOutOfMemoryError("No enough memory for aggregation");
        }
      }

    }

    // Updates the proper row buffer
    if (agg_fastAggBuffer_0 != null) {
      agg_unsafeRowAggBuffer_0 = agg_fastAggBuffer_0;
    }

    // common sub-expressions

    // evaluate aggregate functions and update aggregation buffers
    agg_doAggregate_sum_0(agg_exprIsNull_1_0, agg_expr_1_0, agg_unsafeRowAggBuffer_0);
    agg_doAggregate_avg_0(agg_exprIsNull_3_0, agg_exprIsNull_2_0, agg_unsafeRowAggBuffer_0, agg_expr_3_0, agg_expr_2_0);
    agg_doAggregate_avg_1(agg_exprIsNull_4_0, agg_exprIsNull_5_0, agg_unsafeRowAggBuffer_0, agg_expr_4_0, agg_expr_5_0);

  }

  private void agg_doAggregate_avg_0(boolean agg_exprIsNull_3_0, boolean agg_exprIsNull_2_0, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0, long agg_expr_3_0, double agg_expr_2_0) throws java.io.IOException {
    boolean agg_isNull_14 = true;
    double agg_value_19 = -1.0;
    boolean agg_isNull_15 = agg_unsafeRowAggBuffer_0.isNullAt(1);
    double agg_value_20 = agg_isNull_15 ?
    -1.0 : (agg_unsafeRowAggBuffer_0.getDouble(1));
    if (!agg_isNull_15) {
      if (!agg_exprIsNull_2_0) {
        agg_isNull_14 = false; // resultCode could change nullability.

        agg_value_19 = agg_value_20 + agg_expr_2_0;

      }

    }
    boolean agg_isNull_17 = true;
    long agg_value_22 = -1L;
    boolean agg_isNull_18 = agg_unsafeRowAggBuffer_0.isNullAt(2);
    long agg_value_23 = agg_isNull_18 ?
    -1L : (agg_unsafeRowAggBuffer_0.getLong(2));
    if (!agg_isNull_18) {
      if (!agg_exprIsNull_3_0) {
        agg_isNull_17 = false; // resultCode could change nullability.

        agg_value_22 = agg_value_23 + agg_expr_3_0;

      }

    }

    if (!agg_isNull_14) {
      agg_unsafeRowAggBuffer_0.setDouble(1, agg_value_19);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(1);
    }

    if (!agg_isNull_17) {
      agg_unsafeRowAggBuffer_0.setLong(2, agg_value_22);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(2);
    }
  }

  private void agg_doAggregateWithKeys_0() throws java.io.IOException {
    while ( inputadapter_input_0.hasNext()) {
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

      boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
      int inputadapter_value_0 = inputadapter_isNull_0 ?
      -1 : (inputadapter_row_0.getInt(0));
      boolean inputadapter_isNull_1 = inputadapter_row_0.isNullAt(1);
      long inputadapter_value_1 = inputadapter_isNull_1 ?
      -1L : (inputadapter_row_0.getLong(1));
      boolean inputadapter_isNull_2 = inputadapter_row_0.isNullAt(2);
      double inputadapter_value_2 = inputadapter_isNull_2 ?
      -1.0 : (inputadapter_row_0.getDouble(2));
      boolean inputadapter_isNull_3 = inputadapter_row_0.isNullAt(3);
      long inputadapter_value_3 = inputadapter_isNull_3 ?
      -1L : (inputadapter_row_0.getLong(3));
      boolean inputadapter_isNull_4 = inputadapter_row_0.isNullAt(4);
      double inputadapter_value_4 = inputadapter_isNull_4 ?
      -1.0 : (inputadapter_row_0.getDouble(4));
      boolean inputadapter_isNull_5 = inputadapter_row_0.isNullAt(5);
      long inputadapter_value_5 = inputadapter_isNull_5 ?
      -1L : (inputadapter_row_0.getLong(5));

      agg_doConsume_0(inputadapter_row_0, inputadapter_value_0, inputadapter_isNull_0, inputadapter_value_1, inputadapter_isNull_1, inputadapter_value_2, inputadapter_isNull_2, inputadapter_value_3, inputadapter_isNull_3, inputadapter_value_4, inputadapter_isNull_4, inputadapter_value_5, inputadapter_isNull_5);
      // shouldStop check is eliminated
    }

    agg_fastHashMapIter_0 = agg_fastHashMap_0.rowIterator();
    agg_mapIter_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).finishAggregate(agg_hashMap_0, agg_sorter_0, ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* peakMemory */), ((org.apache.spark.sql.execution.metric.SQLMetric) references[4] /* spillSize */), ((org.apache.spark.sql.execution.metric.SQLMetric) references[5] /* avgHashProbe */), ((org.apache.spark.sql.execution.metric.SQLMetric) references[6] /* numTasksFallBacked */));

  }

  protected void processNext() throws java.io.IOException {
    if (!agg_initAgg_0) {
      agg_initAgg_0 = true;
      agg_fastHashMap_0 = new agg_FastHashMap_0(((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getTaskContext().taskMemoryManager(), ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getEmptyAggregationBuffer());

      ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).getTaskContext().addTaskCompletionListener(
        new org.apache.spark.util.TaskCompletionListener() {
          @Override
          public void onTaskCompletion(org.apache.spark.TaskContext context) {
            agg_fastHashMap_0.close();
          }
        });

      agg_hashMap_0 = ((org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0] /* plan */).createHashMap();
      long wholestagecodegen_beforeAgg_0 = System.nanoTime();
      agg_doAggregateWithKeys_0();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[8] /* aggTime */).add((System.nanoTime() - wholestagecodegen_beforeAgg_0) / 1000000);
    }
    // output the result

    while ( agg_fastHashMapIter_0.next()) {
      UnsafeRow agg_aggKey_0 = (UnsafeRow) agg_fastHashMapIter_0.getKey();
      UnsafeRow agg_aggBuffer_0 = (UnsafeRow) agg_fastHashMapIter_0.getValue();
      agg_doAggregateWithKeysOutput_0(agg_aggKey_0, agg_aggBuffer_0);

      if (shouldStop()) return;
    }
    agg_fastHashMap_0.close();

    while ( agg_mapIter_0.next()) {
      UnsafeRow agg_aggKey_0 = (UnsafeRow) agg_mapIter_0.getKey();
      UnsafeRow agg_aggBuffer_0 = (UnsafeRow) agg_mapIter_0.getValue();
      agg_doAggregateWithKeysOutput_0(agg_aggKey_0, agg_aggBuffer_0);
      if (shouldStop()) return;
    }
    agg_mapIter_0.close();
    if (agg_sorter_0 == null) {
      agg_hashMap_0.free();
    }
  }

}
