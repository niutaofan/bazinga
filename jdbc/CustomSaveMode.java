package org.apache.spark.sql.execution.customDatasource.jdbc;
import org.apache.spark.annotation.InterfaceStability;
/**
 * Created by angel
 */
/**
 * SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
public enum CustomSaveMode {
    /**
     * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
     * contents of the DataFrame are expected to be appended to existing data.
     *
     * @since 1.3.0
     */
    Append,
    /**
     * 默认的源码中没有update操作，因此重构，添加update
     * */
    Update,
    /**
     * Overwrite mode means that when saving a DataFrame to a data source,
     * if data/table already exists, existing data is expected to be overwritten by the contents of
     * the DataFrame.
     *
     * @since 1.3.0
     */
    Overwrite,
    /**
     * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
     * an exception is expected to be thrown.
     *
     * @since 1.3.0
     */
    ErrorIfExists,
    /**
     * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
     * the save operation is expected to not save the contents of the DataFrame and to not
     * change the existing data.
     *
     * @since 1.3.0
     */
    Ignore
}
