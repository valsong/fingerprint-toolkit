package com.valsong.fingerprint.json


import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Named
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

import java.util.stream.Stream

class JsonToolkitTest {

    private static final Logger log = LogManager.getLogger(JsonToolkitTest.class)

    static Stream<Arguments> data() {

        String requestJson1 = """{"size":20,"query":{"bool":{"filter":[{"term":{"brandId":{"value":9,"boost":1.0}}},{"terms":{"categoryId":[1000123],"boost":1.0}},{"term":{"isDel":{"value":0,"boost":1.0}}},{"term":{"status":{"value":1,"boost":1.0}}},{"term":{"bizType":{"value":0,"boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"_source":{"includes":["id"],"excludes":[]},"sort":[{"id":{"order":"asc"}}]}"""
        String exceptRequestFingerprint1 = """{"size":"?","query":{"bool":{"filter":[{"term":{"brandId":{"value":"?","boost":"?"}}},{"terms":{"categoryId":["?"],"boost":"?"}},{"term":{"bizType":{"value":"?","boost":"?"}}},{"term":{"isDel":{"value":"?","boost":"?"}}},{"term":{"status":{"value":"?","boost":"?"}}}],"adjust_pure_negative":"?","boost":"?"}},"_source":{"includes":["?"],"excludes":[]},"sort":[{"id":{"order":"?"}}]}"""
        String exceptJsonKeys1 = """_source.includes.0,query.bool.adjust_pure_negative,query.bool.boost,query.bool.filter.0.term.brandId.boost,query.bool.filter.0.term.brandId.value,query.bool.filter.1.terms.boost,query.bool.filter.1.terms.categoryId.0,query.bool.filter.2.term.bizType.boost,query.bool.filter.2.term.bizType.value,query.bool.filter.3.term.isDel.boost,query.bool.filter.3.term.isDel.value,query.bool.filter.4.term.status.boost,query.bool.filter.4.term.status.value,size,sort.0.id.order"""
        String exceptFingerprintId1 = """8c869dc621b45bf99a074c24020bed936b138cc0da81f91386fe45b1c83d4aca"""


        String requestJson2 = """{"doc_as_upsert":true,"upsert":{"one_spu_one_merchant_merchant_id_pre_sale":0,"one_spu_one_merchant_merchant_id":[0],"has_one_spu_one_merchant_deposit_pre_sale":0,"has_one_spu_one_merchant_brand_offer":0,"one_spu_one_merchant_merchant_id_deposit_pre_sale":0,"has_one_spu_one_merchant_buy_now":0,"one_spu_one_merchant_merchant_id_brand_offer_official":0,"has_one_spu_one_merchant_limit_discounts":0,"one_spu_one_merchant_merchant_id_brand_offer_bonded":0,"spu_id":1001219944,"has_one_spu_one_merchant_flash_send":0,"has_one_spu_one_merchant_oversea_send":0,"one_spu_one_merchant_merchant_id_brand_offer":0,"has_one_spu_one_merchant_rapid_send":0,"has_one_spu_one_merchant_brand_offer_bonded":0,"one_spu_one_merchant_merchant_id_flash_send":0,"has_one_spu_one_merchant_pre_sale":0,"has_one_spu_one_merchant":0,"one_spu_one_merchant_merchant_id_buy_now":0,"one_spu_one_merchant_merchant_id_oversea_send":0,"one_spu_one_merchant_all_merchant_id":[0],"has_one_spu_one_merchant_brand_offer_official":0,"documentId":"1001219944","one_spu_one_merchant_merchant_id_limit_discounts":0,"one_spu_one_merchant_merchant_id_rapid_send":0},"doc":{"one_spu_one_merchant_merchant_id_pre_sale":0,"one_spu_one_merchant_merchant_id":[0],"has_one_spu_one_merchant_deposit_pre_sale":0,"has_one_spu_one_merchant_brand_offer":0,"one_spu_one_merchant_merchant_id_deposit_pre_sale":0,"has_one_spu_one_merchant_buy_now":0,"one_spu_one_merchant_merchant_id_brand_offer_official":0,"has_one_spu_one_merchant_limit_discounts":0,"one_spu_one_merchant_merchant_id_brand_offer_bonded":0,"spu_id":1001219944,"has_one_spu_one_merchant_flash_send":0,"has_one_spu_one_merchant_oversea_send":0,"one_spu_one_merchant_merchant_id_brand_offer":0,"has_one_spu_one_merchant_rapid_send":0,"has_one_spu_one_merchant_brand_offer_bonded":0,"one_spu_one_merchant_merchant_id_flash_send":0,"has_one_spu_one_merchant_pre_sale":0,"has_one_spu_one_merchant":0,"one_spu_one_merchant_merchant_id_buy_now":0,"one_spu_one_merchant_merchant_id_oversea_send":0,"one_spu_one_merchant_all_merchant_id":[0],"has_one_spu_one_merchant_brand_offer_official":0,"documentId":"1001219944","one_spu_one_merchant_merchant_id_limit_discounts":0,"one_spu_one_merchant_merchant_id_rapid_send":0}}"""
        String exceptRequestFingerprint2 = """{"doc_as_upsert":"?","upsert":{"one_spu_one_merchant_merchant_id_pre_sale":"?","one_spu_one_merchant_merchant_id":["?"],"has_one_spu_one_merchant_deposit_pre_sale":"?","has_one_spu_one_merchant_brand_offer":"?","one_spu_one_merchant_merchant_id_deposit_pre_sale":"?","has_one_spu_one_merchant_buy_now":"?","one_spu_one_merchant_merchant_id_brand_offer_official":"?","has_one_spu_one_merchant_limit_discounts":"?","one_spu_one_merchant_merchant_id_brand_offer_bonded":"?","spu_id":"?","has_one_spu_one_merchant_flash_send":"?","has_one_spu_one_merchant_oversea_send":"?","one_spu_one_merchant_merchant_id_brand_offer":"?","has_one_spu_one_merchant_rapid_send":"?","has_one_spu_one_merchant_brand_offer_bonded":"?","one_spu_one_merchant_merchant_id_flash_send":"?","has_one_spu_one_merchant_pre_sale":"?","has_one_spu_one_merchant":"?","one_spu_one_merchant_merchant_id_buy_now":"?","one_spu_one_merchant_merchant_id_oversea_send":"?","one_spu_one_merchant_all_merchant_id":["?"],"has_one_spu_one_merchant_brand_offer_official":"?","documentId":"?","one_spu_one_merchant_merchant_id_limit_discounts":"?","one_spu_one_merchant_merchant_id_rapid_send":"?"},"doc":{"one_spu_one_merchant_merchant_id_pre_sale":"?","one_spu_one_merchant_merchant_id":["?"],"has_one_spu_one_merchant_deposit_pre_sale":"?","has_one_spu_one_merchant_brand_offer":"?","one_spu_one_merchant_merchant_id_deposit_pre_sale":"?","has_one_spu_one_merchant_buy_now":"?","one_spu_one_merchant_merchant_id_brand_offer_official":"?","has_one_spu_one_merchant_limit_discounts":"?","one_spu_one_merchant_merchant_id_brand_offer_bonded":"?","spu_id":"?","has_one_spu_one_merchant_flash_send":"?","has_one_spu_one_merchant_oversea_send":"?","one_spu_one_merchant_merchant_id_brand_offer":"?","has_one_spu_one_merchant_rapid_send":"?","has_one_spu_one_merchant_brand_offer_bonded":"?","one_spu_one_merchant_merchant_id_flash_send":"?","has_one_spu_one_merchant_pre_sale":"?","has_one_spu_one_merchant":"?","one_spu_one_merchant_merchant_id_buy_now":"?","one_spu_one_merchant_merchant_id_oversea_send":"?","one_spu_one_merchant_all_merchant_id":["?"],"has_one_spu_one_merchant_brand_offer_official":"?","documentId":"?","one_spu_one_merchant_merchant_id_limit_discounts":"?","one_spu_one_merchant_merchant_id_rapid_send":"?"}}"""
        String exceptJsonKeys2 = """doc.documentId,doc.has_one_spu_one_merchant,doc.has_one_spu_one_merchant_brand_offer,doc.has_one_spu_one_merchant_brand_offer_bonded,doc.has_one_spu_one_merchant_brand_offer_official,doc.has_one_spu_one_merchant_buy_now,doc.has_one_spu_one_merchant_deposit_pre_sale,doc.has_one_spu_one_merchant_flash_send,doc.has_one_spu_one_merchant_limit_discounts,doc.has_one_spu_one_merchant_oversea_send,doc.has_one_spu_one_merchant_pre_sale,doc.has_one_spu_one_merchant_rapid_send,doc.one_spu_one_merchant_all_merchant_id.0,doc.one_spu_one_merchant_merchant_id.0,doc.one_spu_one_merchant_merchant_id_brand_offer,doc.one_spu_one_merchant_merchant_id_brand_offer_bonded,doc.one_spu_one_merchant_merchant_id_brand_offer_official,doc.one_spu_one_merchant_merchant_id_buy_now,doc.one_spu_one_merchant_merchant_id_deposit_pre_sale,doc.one_spu_one_merchant_merchant_id_flash_send,doc.one_spu_one_merchant_merchant_id_limit_discounts,doc.one_spu_one_merchant_merchant_id_oversea_send,doc.one_spu_one_merchant_merchant_id_pre_sale,doc.one_spu_one_merchant_merchant_id_rapid_send,doc.spu_id,doc_as_upsert,upsert.documentId,upsert.has_one_spu_one_merchant,upsert.has_one_spu_one_merchant_brand_offer,upsert.has_one_spu_one_merchant_brand_offer_bonded,upsert.has_one_spu_one_merchant_brand_offer_official,upsert.has_one_spu_one_merchant_buy_now,upsert.has_one_spu_one_merchant_deposit_pre_sale,upsert.has_one_spu_one_merchant_flash_send,upsert.has_one_spu_one_merchant_limit_discounts,upsert.has_one_spu_one_merchant_oversea_send,upsert.has_one_spu_one_merchant_pre_sale,upsert.has_one_spu_one_merchant_rapid_send,upsert.one_spu_one_merchant_all_merchant_id.0,upsert.one_spu_one_merchant_merchant_id.0,upsert.one_spu_one_merchant_merchant_id_brand_offer,upsert.one_spu_one_merchant_merchant_id_brand_offer_bonded,upsert.one_spu_one_merchant_merchant_id_brand_offer_official,upsert.one_spu_one_merchant_merchant_id_buy_now,upsert.one_spu_one_merchant_merchant_id_deposit_pre_sale,upsert.one_spu_one_merchant_merchant_id_flash_send,upsert.one_spu_one_merchant_merchant_id_limit_discounts,upsert.one_spu_one_merchant_merchant_id_oversea_send,upsert.one_spu_one_merchant_merchant_id_pre_sale,upsert.one_spu_one_merchant_merchant_id_rapid_send,upsert.spu_id"""
        String exceptFingerprintId2 = """d68c01a6b9773087146fbb21b7160cbca1356a4456fcc97855ecad084d2903aa"""


        String requestJson3 = """{"query":{"bool":{"filter":[{"terms":{"spuId":[123456,456789],"boost":1.0}},{"terms":{"boost":1.0,"status":[0]}}],"adjust_pure_negative":true,"boost":1.0}}}"""
        String exceptRequestFingerprint3 = """{"query":{"bool":{"filter":[{"terms":{"boost":"?","status":["?"]}},{"terms":{"spuId":["?"],"boost":"?"}}],"adjust_pure_negative":"?","boost":"?"}}}"""
        String exceptJsonKeys3 = """query.bool.adjust_pure_negative,query.bool.boost,query.bool.filter.0.terms.boost,query.bool.filter.0.terms.status.0,query.bool.filter.1.terms.boost,query.bool.filter.1.terms.spuId.0"""
        String exceptFingerprintId3 = """9db8593a166479b33b019a49f162cf1c2ef84da6c33a5f8f7b7cc379e6a367b0"""

        String requestJson4 = """{"size":500,"query":{"bool":{"should":[{"term":{"host.keyword":"perf-es7.elastic.xxx.net:80"}},{"term":{"host.keyword":"elastic-dev.xxx.com"}},{"term":{"host.keyword":"elastic-dev.xxx.com:80"}}]}}}"""
        String exceptRequestFingerprint4 = """{"size":"?","query":{"bool":{"should":[{"term":{"host.keyword":"?"}}]}}}"""
        String exceptJsonKeys4 = """query.bool.should.0.term.host.keyword,size"""
        String exceptFingerprintId4 = """842121c1e6101dfad3693713e16c476853a4272a5f3df59fef7c830ac58a14d1"""


        String requestJson5 = """{"size":500,"query":{"bool":{"should":[{"term":{"host":"perf-es7.elastic.xxx.net:80"}},{"term":{"host.keyword":"elastic-dev.xxx.com"}},{"term":{"host.keyword":"elastic-dev.xxx.com:80"}}]}}}"""
        String exceptRequestFingerprint5 = """{"size":"?","query":{"bool":{"should":[{"term":{"host.keyword":"?"}},{"term":{"host":"?"}}]}}}"""
        String exceptJsonKeys5 = """query.bool.should.0.term.host.keyword,query.bool.should.1.term.host,size"""
        String exceptFingerprintId5 = """a9523c43a2cb5d23176ec9e3b8f384def47772f63fc5ebd85e1a4a7f698ec935"""

        String requestJson6 = """{"size":500,"query":{"bool":{"should":[{"term":{"host.keyword":"perf-es7.elastic.xxx.net:80"}},{"term":{"host":"elastic-dev.xxx.com"}},{"term":{"host":"elastic-dev.xxx.com:80"}}]}}}"""
        String exceptRequestFingerprint6 = """{"size":"?","query":{"bool":{"should":[{"term":{"host.keyword":"?"}},{"term":{"host":"?"}}]}}}"""
        String exceptJsonKeys6 = """query.bool.should.0.term.host.keyword,query.bool.should.1.term.host,size"""
        String exceptFingerprintId6 = """a9523c43a2cb5d23176ec9e3b8f384def47772f63fc5ebd85e1a4a7f698ec935"""

        String requestJson7 = """{"settings":{},"aliases":{}}"""
        String exceptRequestFingerprint7 = """{"settings":{},"aliases":{}}"""
        String exceptJsonKeys7 = """aliases,settings"""
        String exceptFingerprintId7 = """0c1e5060f4b62d02e67cd2f0a7f8f32cc9a5d905ac7b39adecbee67e7c6f1726"""

        String requestJson8 = """{"size":20,"query":{"bool":{}}}"""
        String exceptRequestFingerprint8 = """{"size":"?","query":{"bool":{}}}"""
        String exceptJsonKeys8 = """query.bool,size"""
        String exceptFingerprintId8 = """1a90a9d20bba5dec05f4aa591483469792d6d7d86dcd5304e6451e00106aff11"""

        String requestJson9 = """{}"""
        String exceptRequestFingerprint9 = """{}"""
        String exceptJsonKeys9 = """"""
        String exceptFingerprintId9 = """e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"""

        String requestJson10 = """{"query":{"match_all":{}}}"""
        String exceptRequestFingerprint10 = """{"query":{"match_all":{}}}"""
        String exceptJsonKeys10 = """query.match_all"""
        String exceptFingerprintId10 = """3a26dfb76f728a1e214445d2a556d1c16f5205cbd9df243765e230a5b1fc2585"""

        String requestJson11 = """"""
        String exceptRequestFingerprint11 = """NONE"""
        String exceptJsonKeys11 = """NONE"""
        String exceptFingerprintId11 = """NONE"""


        String requestJson12 = """{"""
        String exceptRequestFingerprint12 = """ERROR"""
        String exceptJsonKeys12 = """ERROR"""
        String exceptFingerprintId12 = """ERROR"""

        String requestJson13 = """{"aggregations":{"trend_num":{"cardinality":{"field":"t_trend_id"}}},"query":{"bool":{"must":[{"range":{"t_trend_publish_time":{"from":1656604800000,"include_lower":false,"include_upper":true,"boost":1.0}}},{"range":{"t_trend_publish_time":{"to":1788105600000,"include_lower":true,"include_upper":false,"boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"size":0}"""
        String exceptRequestFingerprint13 = """{"aggregations":{"trend_num":{"cardinality":{"field":"?"}}},"query":{"bool":{"must":[{"range":{"t_trend_publish_time":{"to":"?","include_lower":"?","include_upper":"?","boost":"?"}}},{"range":{"t_trend_publish_time":{"from":"?","include_lower":"?","include_upper":"?","boost":"?"}}}],"adjust_pure_negative":"?","boost":"?"}},"size":"?"}"""
        String exceptJsonKeys13 = """aggregations.trend_num.cardinality.field,query.bool.adjust_pure_negative,query.bool.boost,query.bool.must.0.range.t_trend_publish_time.boost,query.bool.must.0.range.t_trend_publish_time.include_lower,query.bool.must.0.range.t_trend_publish_time.include_upper,query.bool.must.0.range.t_trend_publish_time.to,query.bool.must.1.range.t_trend_publish_time.boost,query.bool.must.1.range.t_trend_publish_time.from,query.bool.must.1.range.t_trend_publish_time.include_lower,query.bool.must.1.range.t_trend_publish_time.include_upper,size"""
        String exceptFingerprintId13 = """f01f57912daf61eaafcb4d9c0a12db54b494fadc9812a8ff591762d2a2c08841"""

        String requestJson14 = """{"size":0,"query":{"bool":{"must":[{"range":{"t_trend_publish_time":{"from":1656604800000,"to":null,"include_lower":false,"include_upper":true,"boost":1.0}}},{"range":{"t_trend_publish_time":{"from":null,"to":1788105600000,"include_lower":true,"include_upper":false,"boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"aggregations":{"trend_num":{"cardinality":{"field":"t_trend_id"}}}}"""
        String exceptRequestFingerprint14 = """{"size":"?","query":{"bool":{"must":[{"range":{"t_trend_publish_time":{"from":"?","to":"?","include_lower":"?","include_upper":"?","boost":"?"}}}],"adjust_pure_negative":"?","boost":"?"}},"aggregations":{"trend_num":{"cardinality":{"field":"?"}}}}"""
        String exceptJsonKeys14 = """aggregations.trend_num.cardinality.field,query.bool.adjust_pure_negative,query.bool.boost,query.bool.must.0.range.t_trend_publish_time.boost,query.bool.must.0.range.t_trend_publish_time.from,query.bool.must.0.range.t_trend_publish_time.include_lower,query.bool.must.0.range.t_trend_publish_time.include_upper,query.bool.must.0.range.t_trend_publish_time.to,size"""
        String exceptFingerprintId14 = """c0b1c48f000c7b949b40f26b20fc69ffe6d9173de02be10ea29aaec930996057"""

        return Stream.of(
                Arguments.of(Named.named("普通请求", requestJson1), exceptRequestFingerprint1, exceptJsonKeys1, exceptFingerprintId1),
                Arguments.of(Named.named("普通请求2", requestJson2), exceptRequestFingerprint2, exceptJsonKeys2, exceptFingerprintId2),
                Arguments.of(Named.named("类in查询", requestJson3), exceptRequestFingerprint3, exceptJsonKeys3, exceptFingerprintId3),
                Arguments.of(Named.named("多条件查询，结构体一致", requestJson4), exceptRequestFingerprint4, exceptJsonKeys4, exceptFingerprintId4),
                Arguments.of(Named.named("多条件查询，结构体不完全一致", requestJson5), exceptRequestFingerprint5, exceptJsonKeys5, exceptFingerprintId5),
                Arguments.of(Named.named("多条件查询，结构体不完全一致2", requestJson6), exceptRequestFingerprint6, exceptJsonKeys6, exceptFingerprintId6),
                Arguments.of(Named.named("含有{}1", requestJson7), exceptRequestFingerprint7, exceptJsonKeys7, exceptFingerprintId7),
                Arguments.of(Named.named("含有{}2", requestJson8), exceptRequestFingerprint8, exceptJsonKeys8, exceptFingerprintId8),
                Arguments.of(Named.named("含有{}3", requestJson9), exceptRequestFingerprint9, exceptJsonKeys9, exceptFingerprintId9),
                Arguments.of(Named.named("含有{}4", requestJson10), exceptRequestFingerprint10, exceptJsonKeys10, exceptFingerprintId10),
                Arguments.of(Named.named("body为空", requestJson11), exceptRequestFingerprint11, exceptJsonKeys11, exceptFingerprintId11),
                Arguments.of(Named.named("JSON错误", requestJson12), exceptRequestFingerprint12, exceptJsonKeys12, exceptFingerprintId12),
                Arguments.of(Named.named("数组未压缩", requestJson13), exceptRequestFingerprint13, exceptJsonKeys13, exceptFingerprintId13),
                Arguments.of(Named.named("数组压缩", requestJson14), exceptRequestFingerprint14, exceptJsonKeys14, exceptFingerprintId14),
        )
    }


    @MethodSource("data")
    @ParameterizedTest(name = "[{index}] {0}")
    void fingerprintIdTest(String json, String exceptFingerprint, String exceptJsonKeys, String exceptFingerprintId) throws IOException {

        String fingerprint = JsonToolkit.fingerprint(json)
        String jsonKeys = JsonToolkit.jsonKeys(json)
        String fingerprintId = JsonToolkit.fingerprintId(json)

        log.info("json:[{}]", json)
        log.info("fingerprint:[{}]", fingerprint)
        log.info("jsonKeys:[{}]", jsonKeys)
        log.info("fingerprintId:[{}]", fingerprintId)

        Assertions.assertEquals(exceptFingerprint, fingerprint)
        Assertions.assertEquals(exceptJsonKeys, jsonKeys)
        Assertions.assertEquals(exceptFingerprintId, fingerprintId)

    }

}