package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;

// from explain codegen select sum(ss_ticket_number), ss_quantity, avg(ss_ticket_number), avg(ss_wholesale_cost)
// from store_sales where ss_sold_date_sk=2452640 group by ss_quantity;
//
// field types:
//    ss_ticket_number    	bigint
//    ss_quantity         	int
//    ss_wholesale_cost   	decimal(7,2)

// Found 2 WholeStageCodegen subtrees.
// == Subtree 1 / 2 (maxMethodCodeSize:372; maxConstantPoolSize:340(0.52% used); numInnerClasses:2) ==
// *(1) HashAggregate(keys=[ss_quantity#248], functions=[partial_sum(ss_ticket_number#247L), partial_avg(ss_ticket_number#247L), partial_avg(UnscaledValue(ss_wholesale_cost#249))], output=[ss_quantity#248, sum#353L, sum#354, count#355L, sum#356, count#357L])
// +- *(1) Project [ss_ticket_number#247L, ss_quantity#248, ss_wholesale_cost#249]
//    +- *(1) ColumnarToRow
//       +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.store_sales[ss_ticket_number#247L,ss_quantity#248,ss_wholesale_cost#249,ss_sold_date_sk#261] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,..., PartitionFilters: [isnotnull(ss_sold_date_sk#261), (ss_sold_date_sk#261 = 2452640)], PushedFilters: [], ReadSchema: struct<ss_ticket_number:bigint,ss_quantity:int,ss_wholesale_cost:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage1(references);
// }

// codegenStageId=1
final class GroupStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
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
  private int columnartorow_batchIdx_0;
  private boolean agg_agg_isNull_12_0;
  private boolean agg_agg_isNull_14_0;
  private boolean agg_agg_isNull_21_0;
  private boolean agg_agg_isNull_34_0;
  private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] columnartorow_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[4];
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] columnartorow_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[5];
  private org.apache.spark.sql.vectorized.ColumnarBatch[] columnartorow_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
  private scala.collection.Iterator[] columnartorow_mutableStateArray_0 = new scala.collection.Iterator[1];

  public GroupStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    columnartorow_mutableStateArray_0[0] = inputs[0];
    columnartorow_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 0);
    columnartorow_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
    columnartorow_mutableStateArray_3[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
    columnartorow_mutableStateArray_3[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    columnartorow_mutableStateArray_3[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(6, 0);

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

  private void agg_doAggregate_sum_0(long agg_expr_0_0, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0, boolean agg_exprIsNull_0_0) throws java.io.IOException {
    agg_agg_isNull_12_0 = true;
    long agg_value_17 = -1L;
    do {
      boolean agg_isNull_13 = true;
      long agg_value_18 = -1L;
      agg_agg_isNull_14_0 = true;
      long agg_value_19 = -1L;
      do {
        boolean agg_isNull_15 = agg_unsafeRowAggBuffer_0.isNullAt(0);
        long agg_value_20 = agg_isNull_15 ?
        -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
        if (!agg_isNull_15) {
          agg_agg_isNull_14_0 = false;
          agg_value_19 = agg_value_20;
          continue;
        }

        if (!false) {
          agg_agg_isNull_14_0 = false;
          agg_value_19 = 0L;
          continue;
        }

      } while (false);

      if (!agg_exprIsNull_0_0) {
        agg_isNull_13 = false; // resultCode could change nullability.

        agg_value_18 = agg_value_19 + agg_expr_0_0;

      }
      if (!agg_isNull_13) {
        agg_agg_isNull_12_0 = false;
        agg_value_17 = agg_value_18;
        continue;
      }

      boolean agg_isNull_18 = agg_unsafeRowAggBuffer_0.isNullAt(0);
      long agg_value_23 = agg_isNull_18 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
      if (!agg_isNull_18) {
        agg_agg_isNull_12_0 = false;
        agg_value_17 = agg_value_23;
        continue;
      }

    } while (false);

    if (!agg_agg_isNull_12_0) {
      agg_unsafeRowAggBuffer_0.setLong(0, agg_value_17);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(0);
    }
  }

  private void agg_doAggregateWithKeysOutput_0(UnsafeRow agg_keyTerm_0, UnsafeRow agg_bufferTerm_0)
  throws java.io.IOException {
    ((org.apache.spark.sql.execution.metric.SQLMetric) references[9] /* numOutputRows */).add(1);

    boolean agg_isNull_43 = agg_keyTerm_0.isNullAt(0);
    int agg_value_48 = agg_isNull_43 ?
    -1 : (agg_keyTerm_0.getInt(0));
    boolean agg_isNull_44 = agg_bufferTerm_0.isNullAt(0);
    long agg_value_49 = agg_isNull_44 ?
    -1L : (agg_bufferTerm_0.getLong(0));
    boolean agg_isNull_45 = agg_bufferTerm_0.isNullAt(1);
    double agg_value_50 = agg_isNull_45 ?
    -1.0 : (agg_bufferTerm_0.getDouble(1));
    boolean agg_isNull_46 = agg_bufferTerm_0.isNullAt(2);
    long agg_value_51 = agg_isNull_46 ?
    -1L : (agg_bufferTerm_0.getLong(2));
    boolean agg_isNull_47 = agg_bufferTerm_0.isNullAt(3);
    double agg_value_52 = agg_isNull_47 ?
    -1.0 : (agg_bufferTerm_0.getDouble(3));
    boolean agg_isNull_48 = agg_bufferTerm_0.isNullAt(4);
    long agg_value_53 = agg_isNull_48 ?
    -1L : (agg_bufferTerm_0.getLong(4));

    columnartorow_mutableStateArray_3[4].reset();

    columnartorow_mutableStateArray_3[4].zeroOutNullBytes();

    if (agg_isNull_43) {
      columnartorow_mutableStateArray_3[4].setNullAt(0);
    } else {
      columnartorow_mutableStateArray_3[4].write(0, agg_value_48);
    }

    if (agg_isNull_44) {
      columnartorow_mutableStateArray_3[4].setNullAt(1);
    } else {
      columnartorow_mutableStateArray_3[4].write(1, agg_value_49);
    }

    if (agg_isNull_45) {
      columnartorow_mutableStateArray_3[4].setNullAt(2);
    } else {
      columnartorow_mutableStateArray_3[4].write(2, agg_value_50);
    }

    if (agg_isNull_46) {
      columnartorow_mutableStateArray_3[4].setNullAt(3);
    } else {
      columnartorow_mutableStateArray_3[4].write(3, agg_value_51);
    }

    if (agg_isNull_47) {
      columnartorow_mutableStateArray_3[4].setNullAt(4);
    } else {
      columnartorow_mutableStateArray_3[4].write(4, agg_value_52);
    }

    if (agg_isNull_48) {
      columnartorow_mutableStateArray_3[4].setNullAt(5);
    } else {
      columnartorow_mutableStateArray_3[4].write(5, agg_value_53);
    }
    append((columnartorow_mutableStateArray_3[4].getRow()));

  }

  private void agg_doAggregate_avg_1(boolean agg_isNull_10, long agg_value_15, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0) throws java.io.IOException {
    boolean agg_isNull_32 = true;
    double agg_value_37 = -1.0;
    boolean agg_isNull_33 = agg_unsafeRowAggBuffer_0.isNullAt(3);
    double agg_value_38 = agg_isNull_33 ?
    -1.0 : (agg_unsafeRowAggBuffer_0.getDouble(3));
    if (!agg_isNull_33) {
      agg_agg_isNull_34_0 = true;
      double agg_value_39 = -1.0;
      do {
        boolean agg_isNull_35 = agg_isNull_10;
        double agg_value_40 = -1.0;
        if (!agg_isNull_10) {
          agg_value_40 = (double) agg_value_15;
        }
        if (!agg_isNull_35) {
          agg_agg_isNull_34_0 = false;
          agg_value_39 = agg_value_40;
          continue;
        }

        if (!false) {
          agg_agg_isNull_34_0 = false;
          agg_value_39 = 0.0D;
          continue;
        }

      } while (false);

      agg_isNull_32 = false; // resultCode could change nullability.

      agg_value_37 = agg_value_38 + agg_value_39;

    }
    boolean agg_isNull_37 = false;
    long agg_value_42 = -1L;
    if (!false && agg_isNull_10) {
      boolean agg_isNull_39 = agg_unsafeRowAggBuffer_0.isNullAt(4);
      long agg_value_44 = agg_isNull_39 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(4));
      agg_isNull_37 = agg_isNull_39;
      agg_value_42 = agg_value_44;
    } else {
      boolean agg_isNull_40 = true;
      long agg_value_45 = -1L;
      boolean agg_isNull_41 = agg_unsafeRowAggBuffer_0.isNullAt(4);
      long agg_value_46 = agg_isNull_41 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(4));
      if (!agg_isNull_41) {
        agg_isNull_40 = false; // resultCode could change nullability.

        agg_value_45 = agg_value_46 + 1L;

      }
      agg_isNull_37 = agg_isNull_40;
      agg_value_42 = agg_value_45;
    }

    if (!agg_isNull_32) {
      agg_unsafeRowAggBuffer_0.setDouble(3, agg_value_37);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(3);
    }

    if (!agg_isNull_37) {
      agg_unsafeRowAggBuffer_0.setLong(4, agg_value_42);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(4);
    }
  }

  private void columnartorow_nextBatch_0() throws java.io.IOException {
    if (columnartorow_mutableStateArray_0[0].hasNext()) {
      columnartorow_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch)columnartorow_mutableStateArray_0[0].next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[8] /* numInputBatches */).add(1);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[7] /* numOutputRows */).add(columnartorow_mutableStateArray_1[0].numRows());
      columnartorow_batchIdx_0 = 0;
      columnartorow_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(0);
      columnartorow_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(1);
      columnartorow_mutableStateArray_2[2] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(2);
      columnartorow_mutableStateArray_2[3] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(3);

    }
  }

  private void agg_doConsume_0(long agg_expr_0_0, boolean agg_exprIsNull_0_0, int agg_expr_1_0, boolean agg_exprIsNull_1_0, Decimal agg_expr_2_0, boolean agg_exprIsNull_2_0) throws java.io.IOException {
    UnsafeRow agg_unsafeRowAggBuffer_0 = null;
    UnsafeRow agg_fastAggBuffer_0 = null;

    if (!agg_exprIsNull_1_0) {
      agg_fastAggBuffer_0 = agg_fastHashMap_0.findOrInsert(
        agg_expr_1_0);
    }
    // Cannot find the key in fast hash map, try regular hash map.
    if (agg_fastAggBuffer_0 == null) {
      // generate grouping key
      columnartorow_mutableStateArray_3[3].reset();

      columnartorow_mutableStateArray_3[3].zeroOutNullBytes();

      if (agg_exprIsNull_1_0) {
        columnartorow_mutableStateArray_3[3].setNullAt(0);
      } else {
        columnartorow_mutableStateArray_3[3].write(0, agg_expr_1_0);
      }
      int agg_unsafeRowKeyHash_0 = (columnartorow_mutableStateArray_3[3].getRow()).hashCode();
      if (true) {
        // try to get the buffer from hash map
        agg_unsafeRowAggBuffer_0 =
        agg_hashMap_0.getAggregationBufferFromUnsafeRow((columnartorow_mutableStateArray_3[3].getRow()), agg_unsafeRowKeyHash_0);
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
          (columnartorow_mutableStateArray_3[3].getRow()), agg_unsafeRowKeyHash_0);
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

    boolean agg_isNull_10 = agg_exprIsNull_2_0;
    long agg_value_15 = -1L;

    if (!agg_exprIsNull_2_0) {
      agg_value_15 = agg_expr_2_0.toUnscaledLong();
    }

    // evaluate aggregate functions and update aggregation buffers
    agg_doAggregate_sum_0(agg_expr_0_0, agg_unsafeRowAggBuffer_0, agg_exprIsNull_0_0);
    agg_doAggregate_avg_0(agg_expr_0_0, agg_unsafeRowAggBuffer_0, agg_exprIsNull_0_0);
    agg_doAggregate_avg_1(agg_isNull_10, agg_value_15, agg_unsafeRowAggBuffer_0);

  }

  private void agg_doAggregate_avg_0(long agg_expr_0_0, org.apache.spark.sql.catalyst.InternalRow agg_unsafeRowAggBuffer_0, boolean agg_exprIsNull_0_0) throws java.io.IOException {
    boolean agg_isNull_19 = true;
    double agg_value_24 = -1.0;
    boolean agg_isNull_20 = agg_unsafeRowAggBuffer_0.isNullAt(1);
    double agg_value_25 = agg_isNull_20 ?
    -1.0 : (agg_unsafeRowAggBuffer_0.getDouble(1));
    if (!agg_isNull_20) {
      agg_agg_isNull_21_0 = true;
      double agg_value_26 = -1.0;
      do {
        boolean agg_isNull_22 = agg_exprIsNull_0_0;
        double agg_value_27 = -1.0;
        if (!agg_exprIsNull_0_0) {
          agg_value_27 = (double) agg_expr_0_0;
        }
        if (!agg_isNull_22) {
          agg_agg_isNull_21_0 = false;
          agg_value_26 = agg_value_27;
          continue;
        }

        if (!false) {
          agg_agg_isNull_21_0 = false;
          agg_value_26 = 0.0D;
          continue;
        }

      } while (false);

      agg_isNull_19 = false; // resultCode could change nullability.

      agg_value_24 = agg_value_25 + agg_value_26;

    }
    boolean agg_isNull_25 = false;
    long agg_value_30 = -1L;
    if (!false && agg_exprIsNull_0_0) {
      boolean agg_isNull_28 = agg_unsafeRowAggBuffer_0.isNullAt(2);
      long agg_value_33 = agg_isNull_28 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(2));
      agg_isNull_25 = agg_isNull_28;
      agg_value_30 = agg_value_33;
    } else {
      boolean agg_isNull_29 = true;
      long agg_value_34 = -1L;
      boolean agg_isNull_30 = agg_unsafeRowAggBuffer_0.isNullAt(2);
      long agg_value_35 = agg_isNull_30 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(2));
      if (!agg_isNull_30) {
        agg_isNull_29 = false; // resultCode could change nullability.

        agg_value_34 = agg_value_35 + 1L;

      }
      agg_isNull_25 = agg_isNull_29;
      agg_value_30 = agg_value_34;
    }

    if (!agg_isNull_19) {
      agg_unsafeRowAggBuffer_0.setDouble(1, agg_value_24);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(1);
    }

    if (!agg_isNull_25) {
      agg_unsafeRowAggBuffer_0.setLong(2, agg_value_30);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(2);
    }
  }

  private void agg_doAggregateWithKeys_0() throws java.io.IOException {
    if (columnartorow_mutableStateArray_1[0] == null) {
      columnartorow_nextBatch_0();
    }
    while ( columnartorow_mutableStateArray_1[0] != null) {
      int columnartorow_numRows_0 = columnartorow_mutableStateArray_1[0].numRows();
      int columnartorow_localEnd_0 = columnartorow_numRows_0 - columnartorow_batchIdx_0;
      for (int columnartorow_localIdx_0 = 0; columnartorow_localIdx_0 < columnartorow_localEnd_0; columnartorow_localIdx_0++) {
        int columnartorow_rowIdx_0 = columnartorow_batchIdx_0 + columnartorow_localIdx_0;
        // common sub-expressions

        boolean columnartorow_isNull_0 = columnartorow_mutableStateArray_2[0].isNullAt(columnartorow_rowIdx_0);
        long columnartorow_value_0 = columnartorow_isNull_0 ? -1L : (columnartorow_mutableStateArray_2[0].getLong(columnartorow_rowIdx_0));
        boolean columnartorow_isNull_1 = columnartorow_mutableStateArray_2[1].isNullAt(columnartorow_rowIdx_0);
        int columnartorow_value_1 = columnartorow_isNull_1 ? -1 : (columnartorow_mutableStateArray_2[1].getInt(columnartorow_rowIdx_0));
        boolean columnartorow_isNull_2 = columnartorow_mutableStateArray_2[2].isNullAt(columnartorow_rowIdx_0);
        Decimal columnartorow_value_2 = columnartorow_isNull_2 ? null : (columnartorow_mutableStateArray_2[2].getDecimal(columnartorow_rowIdx_0, 7, 2));

        agg_doConsume_0(columnartorow_value_0, columnartorow_isNull_0, columnartorow_value_1, columnartorow_isNull_1, columnartorow_value_2, columnartorow_isNull_2);
        // shouldStop check is eliminated
      }
      columnartorow_batchIdx_0 = columnartorow_numRows_0;
      columnartorow_mutableStateArray_1[0] = null;
      columnartorow_nextBatch_0();
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
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[10] /* aggTime */).add((System.nanoTime() - wholestagecodegen_beforeAgg_0) / 1000000);
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
