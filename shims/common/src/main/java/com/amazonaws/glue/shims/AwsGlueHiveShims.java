package com.amazonaws.glue.shims;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public interface AwsGlueHiveShims {
  
  ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes);

  byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr);

  Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse)
      throws MetaException;

  boolean validateTableName(String name, HiveConf conf);

  boolean requireCalStats(HiveConf conf, Partition oldPart, Partition newPart, Table tbl, EnvironmentContext environmentContext);

  boolean updateTableStatsFast(Database db, Table tbl, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext)
      throws MetaException;
}
