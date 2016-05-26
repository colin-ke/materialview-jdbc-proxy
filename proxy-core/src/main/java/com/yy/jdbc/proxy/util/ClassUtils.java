/**
 * $Revision$
 * $Date$
 * <p/>
 * Copyright (C) 2004-2008 Jive Software. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yy.jdbc.proxy.util;

/**
 * Class加载工具类
 */
public class ClassUtils {

    private static ClassUtils instance = new ClassUtils();

    /**
     * 根据名称加载类
     *
     * @param className
     * @return
     * @throws ClassNotFoundException
     */
    public static Class forName(String className) throws ClassNotFoundException {
        return instance.loadClass(className);
    }

    private ClassUtils() {
    }

    /**
     * 更具类名称加载类
     *
     * @param className
     * @return
     * @throws ClassNotFoundException
     */
    public Class loadClass(String className) throws ClassNotFoundException {
        Class theClass = null;
        try {
            theClass = Class.forName(className);
        } catch (ClassNotFoundException e1) {
            try {
                theClass = Thread.currentThread().getContextClassLoader().loadClass(className);
            } catch (ClassNotFoundException e2) {
                theClass = getClass().getClassLoader().loadClass(className);
            }
        }
        return theClass;
    }
}
