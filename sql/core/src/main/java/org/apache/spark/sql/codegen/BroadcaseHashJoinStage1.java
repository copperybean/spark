package org.apache.spark.sql.codegen;

import org.apache.spark.sql.types.Decimal;

// explain codegen select i.i_brand_id, i.i_class, i.i_item_sk, i.i_item_desc, w.ws_net_paid_inc_ship from item i join web_sales w on i.i_item_sk = w.ws_item_sk where w.ws_sold_date_sk=2452640 limit 10;
// Found 2 WholeStageCodegen subtrees.
// == Subtree 1 / 2 (maxMethodCodeSize:283; maxConstantPoolSize:136(0.21% used); numInnerClasses:0) ==
// *(1) Project [ws_item_sk#24, ws_net_paid_inc_ship#52]
// +- *(1) Filter isnotnull(ws_item_sk#24)
//    +- *(1) ColumnarToRow
//       +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.web_sales[ws_item_sk#24,ws_net_paid_inc_ship#52,ws_sold_date_sk#55] Batched: true, DataFilters: [isnotnull(ws_item_sk#24)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,..., PartitionFilters: [isnotnull(ws_sold_date_sk#55), (ws_sold_date_sk#55 = 2452640)], PushedFilters: [IsNotNull(ws_item_sk)], ReadSchema: struct<ws_item_sk:int,ws_net_paid_inc_ship:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage1(references);
// }

// codegenStageId=1
final class BroadcaseHashJoinStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private int columnartorow_batchIdx_0;
  private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] columnartorow_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[3];
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] columnartorow_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[3];
  private org.apache.spark.sql.vectorized.ColumnarBatch[] columnartorow_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
  private scala.collection.Iterator[] columnartorow_mutableStateArray_0 = new scala.collection.Iterator[1];

  public BroadcaseHashJoinStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    columnartorow_mutableStateArray_0[0] = inputs[0];

    columnartorow_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
    columnartorow_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
    columnartorow_mutableStateArray_3[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);

  }

  private void columnartorow_nextBatch_0() throws java.io.IOException {
    if (columnartorow_mutableStateArray_0[0].hasNext()) {
      columnartorow_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch)columnartorow_mutableStateArray_0[0].next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numInputBatches */).add(1);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(columnartorow_mutableStateArray_1[0].numRows());
      columnartorow_batchIdx_0 = 0;
      columnartorow_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(0);
      columnartorow_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(1);
      columnartorow_mutableStateArray_2[2] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(2);

    }
  }

  protected void processNext() throws java.io.IOException {
    if (columnartorow_mutableStateArray_1[0] == null) {
      columnartorow_nextBatch_0();
    }
    while ( columnartorow_mutableStateArray_1[0] != null) {
      int columnartorow_numRows_0 = columnartorow_mutableStateArray_1[0].numRows();
      int columnartorow_localEnd_0 = columnartorow_numRows_0 - columnartorow_batchIdx_0;
      for (int columnartorow_localIdx_0 = 0; columnartorow_localIdx_0 < columnartorow_localEnd_0; columnartorow_localIdx_0++) {
        int columnartorow_rowIdx_0 = columnartorow_batchIdx_0 + columnartorow_localIdx_0;
        do {
          boolean columnartorow_isNull_0 = columnartorow_mutableStateArray_2[0].isNullAt(columnartorow_rowIdx_0);
          int columnartorow_value_0 = columnartorow_isNull_0 ? -1 : (columnartorow_mutableStateArray_2[0].getInt(columnartorow_rowIdx_0));

          boolean filter_value_2 = !columnartorow_isNull_0;
          if (!filter_value_2) continue;

          ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);

          // common sub-expressions

          boolean columnartorow_isNull_1 = columnartorow_mutableStateArray_2[1].isNullAt(columnartorow_rowIdx_0);
          Decimal columnartorow_value_1 = columnartorow_isNull_1 ? null : (columnartorow_mutableStateArray_2[1].getDecimal(columnartorow_rowIdx_0, 7, 2));
          columnartorow_mutableStateArray_3[2].reset();

          columnartorow_mutableStateArray_3[2].zeroOutNullBytes();

          if (false) {
            columnartorow_mutableStateArray_3[2].setNullAt(0);
          } else {
            columnartorow_mutableStateArray_3[2].write(0, columnartorow_value_0);
          }

          if (columnartorow_isNull_1) {
            columnartorow_mutableStateArray_3[2].setNullAt(1);
          } else {
            columnartorow_mutableStateArray_3[2].write(1, columnartorow_value_1, 7, 2);
          }
          append((columnartorow_mutableStateArray_3[2].getRow()));

        } while(false);
        if (shouldStop()) { columnartorow_batchIdx_0 = columnartorow_rowIdx_0 + 1; return; }
      }
      columnartorow_batchIdx_0 = columnartorow_numRows_0;
      columnartorow_mutableStateArray_1[0] = null;
      columnartorow_nextBatch_0();
    }
  }

}
