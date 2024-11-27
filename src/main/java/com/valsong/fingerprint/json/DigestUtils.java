/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.valsong.fingerprint.json;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class DigestUtils {

    private static final int STREAM_BUFFER_LENGTH = 1024;

    public static final String SHA_256 = "SHA-256";


    /**
     * Returns a <code>MessageDigest</code> for the given <code>algorithm</code>.
     *
     * @param algorithm the name of the algorithm requested. See <a
     *                  href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html#AppA"
     *                  >Appendix A in the Java Cryptography Architecture Reference Guide</a> for information about standard
     *                  algorithm names.
     * @return A digest instance.
     * @throws IllegalArgumentException when a {@link NoSuchAlgorithmException} is caught.
     * @see MessageDigest#getInstance(String)
     */
    public static MessageDigest getDigest(final String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Returns an SHA-256 digest.
     * <p>
     * Throws a <code>RuntimeException</code> on JRE versions prior to 1.4.0.
     * </p>
     *
     * @return An SHA-256 digest instance.
     * @throws IllegalArgumentException when a {@link NoSuchAlgorithmException} is caught, which should never happen because SHA-256 is a
     *                                  built-in algorithm
     */
    public static MessageDigest getSha256Digest() {
        return getDigest(SHA_256);
    }


    /**
     * Calculates the SHA-256 digest and returns the value as a <code>byte[]</code>.
     * <p>
     * Throws a <code>RuntimeException</code> on JRE versions prior to 1.4.0.
     * </p>
     *
     * @param data Data to digest
     * @return SHA-256 digest
     * @since 1.4
     */
    public static byte[] sha256(final byte[] data) {
        return getSha256Digest().digest(data);
    }


    /**
     * Calculates the SHA-256 digest and returns the value as a <code>byte[]</code>.
     * <p>
     * Throws a <code>RuntimeException</code> on JRE versions prior to 1.4.0.
     * </p>
     *
     * @param data Data to digest; converted to bytes using {@link #getBytesUtf8(String)}
     * @return SHA-256 digest
     * @since 1.4
     */
    public static byte[] sha256(final String data) {
        return sha256(getBytesUtf8(data));
    }

    /**
     * Calculates the SHA-256 digest and returns the value as a hex string.
     * <p>
     * Throws a <code>RuntimeException</code> on JRE versions prior to 1.4.0.
     * </p>
     *
     * @param data Data to digest
     * @return SHA-256 digest as a hex string
     * @since 1.4
     */
    public static String sha256Hex(final byte[] data) {
        return Hex.encodeHexString(sha256(data));
    }

    /**
     * Calculates the SHA-256 digest and returns the value as a hex string.
     * <p>
     * Throws a <code>RuntimeException</code> on JRE versions prior to 1.4.0.
     * </p>
     *
     * @param data Data to digest
     * @return SHA-256 digest as a hex string
     * @since 1.4
     */
    public static String sha256Hex(final String data) {
        return Hex.encodeHexString(sha256(data));
    }


    /**
     * Updates the given {@link MessageDigest}.
     *
     * @param messageDigest the {@link MessageDigest} to update
     * @param valueToDigest the value to update the {@link MessageDigest} with
     * @return the updated {@link MessageDigest}
     * @since 1.7
     */
    public static MessageDigest updateDigest(final MessageDigest messageDigest, final byte[] valueToDigest) {
        messageDigest.update(valueToDigest);
        return messageDigest;
    }

    /**
     * Updates the given {@link MessageDigest}.
     *
     * @param messageDigest the {@link MessageDigest} to update
     * @param valueToDigest the value to update the {@link MessageDigest} with
     * @return the updated {@link MessageDigest}
     * @since 1.11
     */
    public static MessageDigest updateDigest(final MessageDigest messageDigest, final ByteBuffer valueToDigest) {
        messageDigest.update(valueToDigest);
        return messageDigest;
    }

    /**
     * Reads through a File and updates the digest for the data
     *
     * @param digest The MessageDigest to use (e.g. MD5)
     * @param data   Data to digest
     * @return the digest
     * @throws IOException On error reading from the stream
     * @since 1.11
     */
    public static MessageDigest updateDigest(final MessageDigest digest, final File data) throws IOException {
        final BufferedInputStream stream = new BufferedInputStream(new FileInputStream(data));
        try {
            return updateDigest(digest, stream);
        } finally {
            stream.close();
        }
    }

    /**
     * Reads through an InputStream and updates the digest for the data
     *
     * @param digest The MessageDigest to use (e.g. MD5)
     * @param data   Data to digest
     * @return the digest
     * @throws IOException On error reading from the stream
     * @since 1.8
     */
    public static MessageDigest updateDigest(final MessageDigest digest, final InputStream data) throws IOException {
        final byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
        int read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);

        while (read > -1) {
            digest.update(buffer, 0, read);
            read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);
        }

        return digest;
    }


    private final MessageDigest messageDigest;


    /**
     * Creates an instance using the provided {@link MessageDigest} parameter.
     * <p>
     * This can then be used to create digests using methods such as
     * {@link #digest(byte[])} .
     *
     * @param digest the {@link MessageDigest} to use
     * @since 1.11
     */
    public DigestUtils(final MessageDigest digest) {
        this.messageDigest = digest;
    }

    /**
     * Creates an instance using the provided {@link MessageDigest} parameter.
     * <p>
     * This can then be used to create digests using methods such as
     * {@link #digest(byte[])} .
     *
     * @param name the name of the {@link MessageDigest} to use
     * @throws IllegalArgumentException when a {@link NoSuchAlgorithmException} is caught.
     * @see #getDigest(String)
     * @since 1.11
     */
    public DigestUtils(final String name) {
        this(getDigest(name));
    }

    /**
     * Returns the message digest instance.
     *
     * @return the message digest instance
     * @since 1.11
     */
    public MessageDigest getMessageDigest() {
        return messageDigest;
    }

    /**
     * Reads through a byte array and returns the digest for the data.
     *
     * @param data Data to digest
     * @return the digest
     * @since 1.11
     */
    public byte[] digest(final byte[] data) {
        return updateDigest(messageDigest, data).digest();
    }


    /**
     * Encodes the given string into a sequence of bytes using the UTF-8 charset, storing the result into a new byte
     * array.
     *
     * @param string the String to encode, may be <code>null</code>
     * @return encoded bytes, or <code>null</code> if the input string was <code>null</code>
     * @see <a href="http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html">Standard charsets</a>
     * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
     */
    public static byte[] getBytesUtf8(final String string) {
        return getBytes(string, StandardCharsets.UTF_8);
    }


    /**
     * Calls {@link String#getBytes(Charset)}
     *
     * @param string  The string to encode (if null, return null).
     * @param charset The {@link Charset} to encode the <code>String</code>
     * @return the encoded bytes
     */
    private static byte[] getBytes(final String string, final Charset charset) {
        if (string == null) {
            return null;
        }
        return string.getBytes(charset);
    }


}
