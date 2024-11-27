package com.valsong.fingerprint.sql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import com.valsong.fingerprint.json.DigestUtils;
import com.valsong.fingerprint.StringUtils;

import static com.valsong.fingerprint.ElasticToolConstants.ERROR;
import static com.valsong.fingerprint.ElasticToolConstants.NONE;


/**
 * SqlToolkit
 *
 * @author Val Song
 */
public class SqlToolkit {


    /**
     * 获取SQL指纹
     *
     * @param sql sql
     * @return SQL指纹
     */
    public static String fingerprint(String sql) {
        try {
            // sharding不合并、and不合并、or不合并、in合并
            // OutputParameterizedQuesUnMergeValuesList 不明白使用场景，暂时不管了
            // OutputParameterizedZeroReplaceNotUseOriginalSql 添加了后，没有查询条件时也会格式化，不加没有条件的时候不会格式化
            return ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql,
                    VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql,
                    VisitorFeature.OutputParameterizedUnMergeShardingTable,
                    VisitorFeature.OutputParameterizedQuesUnMergeAnd,
                    VisitorFeature.OutputParameterizedQuesUnMergeOr);
        } catch (Throwable e) {
            return ERROR;
        }
    }

    /**
     * 获取SQL指纹ID
     *
     * @param sql sql
     * @return SQL指纹ID
     */
    public static final String fingerprintId(String sql) {
        if (StringUtils.isBlank(sql)) {
            return NONE;
        }
        try {
            String fingerprint = fingerprint(sql);
            if (ERROR.equals(fingerprint)) {
                return fingerprint;
            }
            // sha256
            return DigestUtils.sha256Hex(fingerprint);
        } catch (Throwable e) {
            return ERROR;
        }
    }


}
