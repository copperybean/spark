package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;

// public Object generate(Object[] references) {
//   return new SumGroupStage2(references);
// }

// codegenStageId=2
final class SumGroupStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg_0;
  private boolean agg_bufIsNull_0;
  private long agg_bufValue_0;
  private agg_FastHashMap_0 agg_fastHashMap_0;
  private org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> agg_fastHashMapIter_0;
  private org.apache.spark.unsafe.KVIterator agg_mapIter_0;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap_0;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter_0;
  private scala.collection.Iterator inputadapter_input_0;
  private boolean agg_agg_isNull_3_0;
  private boolean agg_agg_isNull_5_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] agg_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public SumGroupStage2(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    inputadapter_input_0 = inputs[0];
    agg_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    agg_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

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
    agg_agg_isNull_3_0 = true;
    long agg_value_4 = -1L;
    do {
      boolean agg_isNull_4 = true;
      long agg_value_5 = -1L;
      agg_agg_isNull_5_0 = true;
      long agg_value_6 = -1L;
      do {
        boolean agg_isNull_6 = agg_unsafeRowAggBuffer_0.isNullAt(0);
        long agg_value_7 = agg_isNull_6 ?
        -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
        if (!agg_isNull_6) {
          agg_agg_isNull_5_0 = false;
          agg_value_6 = agg_value_7;
          continue;
        }

        if (!false) {
          agg_agg_isNull_5_0 = false;
          agg_value_6 = 0L;
          continue;
        }

      } while (false);

      if (!agg_exprIsNull_1_0) {
        agg_isNull_4 = false; // resultCode could change nullability.

        agg_value_5 = agg_value_6 + agg_expr_1_0;

      }
      if (!agg_isNull_4) {
        agg_agg_isNull_3_0 = false;
        agg_value_4 = agg_value_5;
        continue;
      }

      boolean agg_isNull_9 = agg_unsafeRowAggBuffer_0.isNullAt(0);
      long agg_value_10 = agg_isNull_9 ?
      -1L : (agg_unsafeRowAggBuffer_0.getLong(0));
      if (!agg_isNull_9) {
        agg_agg_isNull_3_0 = false;
        agg_value_4 = agg_value_10;
        continue;
      }

    } while (false);

    if (!agg_agg_isNull_3_0) {
      agg_unsafeRowAggBuffer_0.setLong(0, agg_value_4);
    } else {
      agg_unsafeRowAggBuffer_0.setNullAt(0);
    }
  }

  private void agg_doAggregateWithKeysOutput_0(UnsafeRow agg_keyTerm_0, UnsafeRow agg_bufferTerm_0)
  throws java.io.IOException {
    ((org.apache.spark.sql.execution.metric.SQLMetric) references[7] /* numOutputRows */).add(1);

    boolean agg_isNull_10 = agg_keyTerm_0.isNullAt(0);
    int agg_value_11 = agg_isNull_10 ?
    -1 : (agg_keyTerm_0.getInt(0));
    boolean agg_isNull_11 = agg_bufferTerm_0.isNullAt(0);
    long agg_value_12 = agg_isNull_11 ?
    -1L : (agg_bufferTerm_0.getLong(0));

    agg_mutableStateArray_0[1].reset();

    agg_mutableStateArray_0[1].zeroOutNullBytes();

    if (agg_isNull_10) {
      agg_mutableStateArray_0[1].setNullAt(0);
    } else {
      agg_mutableStateArray_0[1].write(0, agg_value_11);
    }

    if (agg_isNull_11) {
      agg_mutableStateArray_0[1].setNullAt(1);
    } else {
      agg_mutableStateArray_0[1].write(1, agg_value_12);
    }
    append((agg_mutableStateArray_0[1].getRow()));

  }

  private void agg_doConsume_0(InternalRow inputadapter_row_0, int agg_expr_0_0, boolean agg_exprIsNull_0_0, long agg_expr_1_0, boolean agg_exprIsNull_1_0) throws java.io.IOException {
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

      agg_doConsume_0(inputadapter_row_0, inputadapter_value_0, inputadapter_isNull_0, inputadapter_value_1, inputadapter_isNull_1);
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
