package org.research.kafkapractice;

import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @fileName: GeneratorToken.java
 * @description: GeneratorToken.java类说明
 * @author: by echo huang
 * @date: 2020-04-27 14:03
 */
public class GeneratorToken {
    private static final String ACCESS_TOKEN = "2ea61155-d1fa-4e88-a181-96a5f5a8cd3f";
    private static final String ACCESS_SECRET = "fengbiancdp";


    public static void main(String[] args) throws Exception {
        System.out.println(generatorToken());
        System.out.println(System.currentTimeMillis());
    }

    /**
     * 得到token
     *
     * @return
     * @throws Exception
     */
    private static String generatorToken() throws Exception {
        String sign = DigestUtils.sha1Hex(jointSign());
        return new String(Base64.getEncoder().encode(jointToken(sign)), StandardCharsets.UTF_8);
    }

    private static String jointSign() {
        return ACCESS_TOKEN + ACCESS_SECRET + System.currentTimeMillis() / 1000;
    }

    private static byte[] jointToken(String sign) {
        return (ACCESS_TOKEN + "," + System.currentTimeMillis() / 1000 + "," + sign).getBytes();
    }
}
