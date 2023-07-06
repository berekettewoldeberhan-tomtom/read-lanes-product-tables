package com.tomtom;

import org.apache.spark.sql.*;

import java.math.BigInteger;
import java.util.List;

public class LanesProductTableReader {

    public static void main(String[] args) {
        LanesProductTableReader deltaLakeReader = new LanesProductTableReader();
        String accountKey = "skeiwllwwkek....";
        deltaLakeReader.readDeltaLakeTable("sacariadseed", accountKey, "lanegroups", "main/lane_borders");
    }

    public void readDeltaLakeTable(String accountName, String accountKey, String containerName, String deltaLakePath) {
        // Spark configuration
        SparkSession spark = SparkSession
                .builder()
                .appName("DeltaLakeReader")
                .master("local[*]") // Use local master for testing, remove this in a real application
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
                .config("spark.hadoop.fs.azure.account.key." + accountName + ".blob.core.windows.net", accountKey)
                .getOrCreate();

        spark.sparkContext().setLogLevel("INFO");

        // Read from Delta Lake table
        String deltaLakeUrl = "wasbs://" + containerName + "@" + accountName + ".blob.core.windows.net/" + deltaLakePath;
        Dataset<Row> deltaLakeData = spark.read().format("delta").load(deltaLakeUrl);

        // Convert to Dataset<MyModel>
        Encoder<LaneBorders> laneBordersEncoder = Encoders.bean(LaneBorders.class);
        Dataset<LaneBorders> laneBordersData = deltaLakeData.as(laneBordersEncoder);

        // Show data
        //deltaLakeData.show();
        laneBordersData.show();
    }

    static class LaneBorders {
        String F_ID;
        String GEOM;
        String BORDER_TYPE;
        List<BigInteger> H3_RESOLUTION_8;

        public LaneBorders(){}

        public LaneBorders(String f_ID, String GEOM, String BORDER_TYPE, List<BigInteger> h3_RESOLUTION_8) {
            F_ID = f_ID;
            this.GEOM = GEOM;
            this.BORDER_TYPE = BORDER_TYPE;
            H3_RESOLUTION_8 = h3_RESOLUTION_8;
        }

        public String getF_ID() {
            return F_ID;
        }

        public void setF_ID(String f_ID) {
            F_ID = f_ID;
        }

        public String getGEOM() {
            return GEOM;
        }

        public void setGEOM(String GEOM) {
            this.GEOM = GEOM;
        }

        public String getBORDER_TYPE() {
            return BORDER_TYPE;
        }

        public void setBORDER_TYPE(String BORDER_TYPE) {
            this.BORDER_TYPE = BORDER_TYPE;
        }

        public List<BigInteger> getH3_RESOLUTION_8() {
            return H3_RESOLUTION_8;
        }

        public void setH3_RESOLUTION_8(List<BigInteger> h3_RESOLUTION_8) {
            H3_RESOLUTION_8 = h3_RESOLUTION_8;
        }
    }
}