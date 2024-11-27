package com.valsong.fingerprint.sql


import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Named
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

import java.util.stream.Stream

class SqlToolkitTest {


    static Stream<Arguments> data() {

        String fingerprint1 = """SELECT id, name
FROM xxx
WHERE name = ?
LIMIT ?"""

        String fingerprintId1 = """ecf169bea9c99fb46ea1c53c32805d25de13acba53800ce1caf2c877dcd9a72b"""

        String sql1 = """select id,name from xxx where name = '123' limit 100 """

        String sql2 = """SELECT   id,name FROM xxx     WHERE name = '123' LIMIT 100 """

        String fingerprint2 = """SELECT `id`, `name`
FROM xxx t
WHERE t.`name` = ?"""

        String fingerprintId2 = """d1c3c54889bfa77cf95ae47f029326ca3c2df2693d6256c25f7daf4e5bd90066"""

        String sql3 = """SELECT `id`,`name` FROM xxx t WHERE t.`name` = 123 """

        String sql4 = """select    `id`,`name`    from xxx t   where   t.`name` = 123 """

        String sql5 = """select * from drop where a = 1 and (b = 2 or c = 3 or d =4)"""
        String fingerprint5 = "ERROR"
        String fingerprintId5 = "ERROR"

        String sql6 = """select * from table where a = 1 and (b = 2 or c = 3 or d =4)"""
        String fingerprint6 = "ERROR"
        String fingerprintId6 = "ERROR"

        String sql7 = """select * from `drop` where a = 1 and (b = 2 or c = 3 or d =4 or d =5)"""
        String fingerprint7 = """SELECT *
FROM `drop`
WHERE a = ?
\tAND (b = ?
\t\tOR c = ?
\t\tOR d = ?
\t\tOR d = ?)"""
        String fingerprintId7 = "d25513ebcc85e500741629b563d526bdd64453da59c4aa4e7f02b2f4078fd5b2"

        String sql8 = """select * from `table` where a = 1 and (b = 2 or c = 3 or d =4)"""
        String fingerprint8 = """SELECT *
FROM `table`
WHERE a = ?
\tAND (b = ?
\t\tOR c = ?
\t\tOR d = ?)"""
        String fingerprintId8 = "323bbe172f1d9f99bd9225ef5ca389080d180008c73a0f1dedbadabd3fde16fb"

        String sql9 = """select requestFingerprintId,count(1)  from es_request_log_20241121  group by requestFingerprintId"""
        String fingerprint9 = """SELECT requestFingerprintId, count(1)
FROM es_request_log_20241121
GROUP BY requestFingerprintId"""
        String fingerprintId9 = "c2ae41ccad85d7cd7c211a547caa09bbcb6f269f8c2e737ae85e798d3311452f"

        String sql10 = """SELECT state,COUNT(*),MAX(age),AVG(balance) FROM account GROUP BY state HAVING COUNT(*) > 15 LIMIT 10"""
        String fingerprint10 = """SELECT state, COUNT(*), MAX(age)
\t, AVG(balance)
FROM account
GROUP BY state
HAVING COUNT(*) > ?
LIMIT ?"""
        String fingerprintId10 = "ac20d2ebf74bcb15fd4501b31203f58f5abdb74e5e235be06e13e66816bcceb2"

        String sql11 = "select `id`,name from test_index_20241121 where name in('elastic','search')"
        String fingerprint11 = """SELECT `id`, name
FROM test_index_20241121
WHERE name IN (?)"""
        String fingerprintId11 = "a7f8a435601c6a75b8b490f321c914f603d1273583b1ade2a5749ddb17893a7e"

        String sql12 = "select `id`,name from test_index_20241121 where (name = 'elastic' or name = 'search') and id = 1 "
        String fingerprint12 = """SELECT `id`, name
FROM test_index_20241121
WHERE (name = ?
\t\tOR name = ?)
\tAND id = ?"""
        String fingerprintId12 = "c1fe6470087bf247a5a618a87f9a5588955c287ea8e91e3b8dcd13394c2e9820"


        String sql13 = "select `id`,name from test_index_20241121 where (id = 1 and name = '1' )and  (id = 2 and name = '2')"
        String fingerprint13 = """SELECT `id`, name
FROM test_index_20241121
WHERE id = ?
\tAND name = ?
\tAND (id = ?
\t\tAND name = ?)"""
        String fingerprintId13 = "f60a81c62302917ce7f4c3d3871f89811d723955b7a2aea6f06b69b2bbc6d6b5"


        String sql14 = "select id,name from test_index_20241121"
        String fingerprint14 = """SELECT id, name
FROM test_index_20241121"""
        String fingerprintId14 = "6dd30fc4b7acf444dbb9c719f34a435d3407f4277c21d9bf6d4d62d10642b962"

        String sql15 = "    SELECT    id,    name    FROM    test_index_20241121    "
        String fingerprint15 = """SELECT id, name
FROM test_index_20241121"""
        String fingerprintId15 = "6dd30fc4b7acf444dbb9c719f34a435d3407f4277c21d9bf6d4d62d10642b962"



        return Stream.of(
                Arguments.of(Named.named("普通SQL1", sql1), fingerprint1, fingerprintId1),
                Arguments.of(Named.named("普通SQL2", sql2), fingerprint1, fingerprintId1),
                Arguments.of(Named.named("普通SQL3", sql3), fingerprint2, fingerprintId2),
                Arguments.of(Named.named("普通SQL4", sql4), fingerprint2, fingerprintId2),
                Arguments.of(Named.named("普通SQL5", sql5), fingerprint5, fingerprintId5),
                Arguments.of(Named.named("普通SQL6", sql6), fingerprint6, fingerprintId6),
                Arguments.of(Named.named("普通SQL7", sql7), fingerprint7, fingerprintId7),
                Arguments.of(Named.named("普通SQL8", sql8), fingerprint8, fingerprintId8),
                Arguments.of(Named.named("验证shardingSQ9", sql9), fingerprint9, fingerprintId9),
                Arguments.of(Named.named("普通SQL10", sql10), fingerprint10, fingerprintId10),
                Arguments.of(Named.named("验证in合并SQL11", sql11), fingerprint11, fingerprintId11),
                Arguments.of(Named.named("验证or不合并SQL12", sql12), fingerprint12, fingerprintId12),
                Arguments.of(Named.named("验证and不合并，但是测试用例有问题SQL13", sql13), fingerprint13, fingerprintId13),
                Arguments.of(Named.named("无查询条件SQL14", sql14), fingerprint14, fingerprintId14),
                Arguments.of(Named.named("无查询条件带空格SQL15", sql15), fingerprint15, fingerprintId15),
        )
    }


    @MethodSource("data")
    @ParameterizedTest(name = "[{index}] {0}")
    void fingerprintIdTest(String sql, String exceptFingerprint, String exceptFingerprintId) {

        String fingerprint = SqlToolkit.fingerprint(sql)

        String fingerprintId = SqlToolkit.fingerprintId(sql)

        println fingerprint

        println fingerprintId

        Assertions.assertEquals(exceptFingerprint, fingerprint)
        Assertions.assertEquals(exceptFingerprintId, fingerprintId)

    }




}
