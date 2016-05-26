package com.yy.jdbc.proxy.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * 序列化工具类
 *
 * @author ZhangXueJun
 */
public class SerializeHelper {

    private static final Logger logger = LoggerFactory.getLogger(SerializeHelper.class);

    /**
     * 序列化
     *
     * @param object
     * @return
     */
    public static byte[] serialize(Serializable object) {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream dos = null;
        try {
            bos = new ByteArrayOutputStream(2048);
            dos = new ObjectOutputStream(bos);
            dos.writeObject(object);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (dos != null) {
                try {
                    dos.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return null;
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @param <T>
     * @return
     */
    public static <T> T deserialize(byte[] bytes) {
        ByteArrayInputStream bin = null;
        ObjectInputStream din = null;
        try {
            bin = new ByteArrayInputStream(bytes);
            din = new ObjectInputStream(bin);
            return (T) din.readObject();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (bin != null) {
                try {
                    bin.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            if (din != null) {
                try {
                    din.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return null;
    }

}
