package org.apache.spark.sql.codegen;

// from explain codegen select sum(sr_addr_sk) from store_returns;

// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage1(references);
// }

// codegenStageId=1
final class SumStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg_0;
  private boolean agg_bufIsNull_0;
  private long agg_bufValue_0;
  private int columnartorow_batchIdx_0;
  private boolean agg_agg_isNull_2_0;
  private boolean agg_agg_isNull_4_0;
  private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] columnartorow_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[2];
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] columnartorow_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[4];
  private org.apache.spark.sql.vectorized.ColumnarBatch[] columnartorow_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
  private scala.collection.Iterator[] columnartorow_mutableStateArray_0 = new scala.collection.Iterator[1];

  public SumStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    columnartorow_mutableStateArray_0[0] = inputs[0];
    columnartorow_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
    columnartorow_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    columnartorow_mutableStateArray_3[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    columnartorow_mutableStateArray_3[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);

  }

  private void agg_doAggregate_sum_0(int agg_expr_0_0, boolean agg_exprIsNull_0_0) throws java.io.IOException {
    agg_agg_isNull_2_0 = true;
    long agg_value_2 = -1L;
    do {
      boolean agg_isNull_3 = true;
      long agg_value_3 = -1L;
      agg_agg_isNull_4_0 = true;
      long agg_value_4 = -1L;
      do {
        if (!agg_bufIsNull_0) {
          agg_agg_isNull_4_0 = false;
          agg_value_4 = agg_bufValue_0;
          continue;
        }

        if (!false) {
          agg_agg_isNull_4_0 = false;
          agg_value_4 = 0L;
          continue;
        }

      } while (false);
      boolean agg_isNull_7 = agg_exprIsNull_0_0;
      long agg_value_7 = -1L;
      if (!agg_exprIsNull_0_0) {
        agg_value_7 = (long) agg_expr_0_0;
      }
      if (!agg_isNull_7) {
        agg_isNull_3 = false; // resultCode could change nullability.

        agg_value_3 = agg_value_4 + agg_value_7;

      }
      if (!agg_isNull_3) {
        agg_agg_isNull_2_0 = false;
        agg_value_2 = agg_value_3;
        continue;
      }

      if (!agg_bufIsNull_0) {
        agg_agg_isNull_2_0 = false;
        agg_value_2 = agg_bufValue_0;
        continue;
      }

    } while (false);

    agg_bufIsNull_0 = agg_agg_isNull_2_0;
    agg_bufValue_0 = agg_value_2;
  }

  private void agg_doAggregateWithoutKey_0() throws java.io.IOException {
    // initialize aggregation buffer
    agg_bufIsNull_0 = true;
    agg_bufValue_0 = -1L;

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
        int columnartorow_value_0 = columnartorow_isNull_0 ? -1 : (columnartorow_mutableStateArray_2[0].getInt(columnartorow_rowIdx_0));

        agg_doConsume_0(columnartorow_value_0, columnartorow_isNull_0);
        // shouldStop check is eliminated
      }
      columnartorow_batchIdx_0 = columnartorow_numRows_0;
      columnartorow_mutableStateArray_1[0] = null;
      columnartorow_nextBatch_0();
    }

  }

  private void columnartorow_nextBatch_0() throws java.io.IOException {
    if (columnartorow_mutableStateArray_0[0].hasNext()) {
      columnartorow_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch)columnartorow_mutableStateArray_0[0].next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numInputBatches */).add(1);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(columnartorow_mutableStateArray_1[0].numRows());
      columnartorow_batchIdx_0 = 0;
      columnartorow_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(0);
      columnartorow_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(1);

    }
  }

  private void agg_doConsume_0(int agg_expr_0_0, boolean agg_exprIsNull_0_0) throws java.io.IOException {
    // do aggregate
    // common sub-expressions

    // evaluate aggregate functions and update aggregation buffers
    agg_doAggregate_sum_0(agg_expr_0_0, agg_exprIsNull_0_0);

  }

  protected void processNext() throws java.io.IOException {
    while (!agg_initAgg_0) {
      agg_initAgg_0 = true;
      long agg_beforeAgg_0 = System.nanoTime();
      agg_doAggregateWithoutKey_0();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* aggTime */).add((System.nanoTime() - agg_beforeAgg_0) / 1000000);

      // output the result

      ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);
      columnartorow_mutableStateArray_3[3].reset();

      columnartorow_mutableStateArray_3[3].zeroOutNullBytes();

      if (agg_bufIsNull_0) {
        columnartorow_mutableStateArray_3[3].setNullAt(0);
      } else {
        columnartorow_mutableStateArray_3[3].write(0, agg_bufValue_0);
      }
      append((columnartorow_mutableStateArray_3[3].getRow()));
    }
  }

}
