package com.newsbreak.data.utils;

import org.apache.commons.lang3.StringUtils;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created on 2020/2/24.
 *
 * @author wei.liu
 */

public class VersionUtils {

    public static final String VERSION_SPLIT_REGEX      = "[._]";

    /**
     * 比较版本号的大小,前者大则返回一个正数,后者大返回一个负数,相等则返回0
     * @param version1
     * @param version2
     * @return
     */
    public static int compareVersion(String version1, String version2) throws Exception {
        if (version1 == null || version2 == null) {
            throw new Exception("compareVersion error:illegal params.");
        }
        String[] versionArray1 = version1.split(VERSION_SPLIT_REGEX);
        String[] versionArray2 = version2.split(VERSION_SPLIT_REGEX);
        int minLength = Math.min(versionArray1.length, versionArray2.length);//取最小长度值

        int idx = 0;
        int diff = 0;
        while (idx < minLength
                && (diff = versionArray1[idx].length() - versionArray2[idx].length()) == 0//先比较长度
                && (diff = versionArray1[idx].compareTo(versionArray2[idx])) == 0) {//再比较字符
            ++idx;
        }
        //如果已经分出大小，则直接返回，如果未分出大小，则再比较位数，有子版本的为大；
        diff = (diff != 0) ? diff : versionArray1.length - versionArray2.length;
        return diff;
    }

}
