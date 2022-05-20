package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;

// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage2(references);
// }

// codegenStageId=2
final class SumStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg_0;
  private boolean agg_bufIsNull_0;
  private long agg_bufValue_0;
  private scala.collection.Iterator inputadapter_input_0;
  private boolean agg_agg_isNull_3_0;
  private boolean agg_agg_isNull_5_0;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] agg_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];

  public SumStage2(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;

    inputadapter_input_0 = inputs[0];
    agg_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);

  }

  private void agg_doAggregateWithoutKey_0() throws java.io.IOException {
    // initialize aggregation buffer
    agg_bufIsNull_0 = true;
    agg_bufValue_0 = -1L;

    while ( inputadapter_input_0.hasNext()) {
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

      boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
      long inputadapter_value_0 = inputadapter_isNull_0 ?
      -1L : (inputadapter_row_0.getLong(0));

      agg_doConsume_0(inputadapter_row_0, inputadapter_value_0, inputadapter_isNull_0);
      // shouldStop check is eliminated
    }

  }

  private void agg_doConsume_0(InternalRow inputadapter_row_0, long agg_expr_0_0, boolean agg_exprIsNull_0_0) throws java.io.IOException {
    // do aggregate
    // common sub-expressions

    // evaluate aggregate functions and update aggregation buffers

    agg_agg_isNull_3_0 = true;
    long agg_value_3 = -1L;
    do {
      boolean agg_isNull_4 = true;
      long agg_value_4 = -1L;
      agg_agg_isNull_5_0 = true;
      long agg_value_5 = -1L;
      do {
        if (!agg_bufIsNull_0) {
          agg_agg_isNull_5_0 = false;
          agg_value_5 = agg_bufValue_0;
          continue;
        }

        if (!false) {
          agg_agg_isNull_5_0 = false;
          agg_value_5 = 0L;
          continue;
        }

      } while (false);

      if (!agg_exprIsNull_0_0) {
        agg_isNull_4 = false; // resultCode could change nullability.

        agg_value_4 = agg_value_5 + agg_expr_0_0;

      }
      if (!agg_isNull_4) {
        agg_agg_isNull_3_0 = false;
        agg_value_3 = agg_value_4;
        continue;
      }

      if (!agg_bufIsNull_0) {
        agg_agg_isNull_3_0 = false;
        agg_value_3 = agg_bufValue_0;
        continue;
      }

    } while (false);

    agg_bufIsNull_0 = agg_agg_isNull_3_0;
    agg_bufValue_0 = agg_value_3;

  }

  protected void processNext() throws java.io.IOException {
    while (!agg_initAgg_0) {
      agg_initAgg_0 = true;
      long agg_beforeAgg_0 = System.nanoTime();
      agg_doAggregateWithoutKey_0();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* aggTime */).add((System.nanoTime() - agg_beforeAgg_0) / 1000000);

      // output the result

      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
      agg_mutableStateArray_0[0].reset();

      agg_mutableStateArray_0[0].zeroOutNullBytes();

      if (agg_bufIsNull_0) {
        agg_mutableStateArray_0[0].setNullAt(0);
      } else {
        agg_mutableStateArray_0[0].write(0, agg_bufValue_0);
      }
      append((agg_mutableStateArray_0[0].getRow()));
    }
  }

}
