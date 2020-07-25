package org.jmqtt.common.helper;

import org.slf4j.Logger;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;


public class MixAll {

    public static String MQTT_VERSION_SUPPORT = "mqtt, mqtt3.1, mqtt3.1.1";

    public static boolean createIfNotExistsDir(File file){
        return file != null && (file.exists() ? file.isDirectory() : file.mkdirs());
    }

    /**
     * 打印属性配置对象obj中的所有字段的键值对
     *
     * @param log
     * @param obj
     */
    public static void printProperties(Logger log, Object obj) {
        Class clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();
        if(fields != null){
            for(Field field : fields){
                try {
                    field.setAccessible(true);
                    String key = field.getName();
                    Object value = field.get(obj);
                    log.info("{} = {}", key, value);
                } catch (IllegalAccessException e) {
                }
            }
        }
    }

    /**
     * transfer properties 2 pojo
     */
    public static void properties2POJO(Properties properties, Object obj){
        Method[] methods = obj.getClass().getMethods();
        if(methods != null){
            for(Method method : methods){
                String methodName = method.getName();
                if(methodName.startsWith("set")){
                    try{
                        // 步骤1：根据setter获取字段名
                        String tmp = methodName.substring(4); // setter中的字段名，首字母为大写
                        String firstChar = methodName.substring(3, 4); // 字段名首字母，一般为大写
                        String key = firstChar.toLowerCase() + tmp; // 通过setter获得实际的字段名，首字母小写

                        // 步骤2：根据字段名，从属性中获取配置值
                        String value = properties.getProperty(key);

                        // 步骤3：调用setter方法，将值设置到相应字段
                        if(value != null){
                            Class<?>[] types = method.getParameterTypes();
                            if(types != null && types.length > 0){
                                String type = types[0].getSimpleName();
                                Object arg = null;
                                // 字段值先转为与字段相同类型的值
                                if(type.equals("int") || type.equals("Integer")){
                                    arg = Integer.parseInt(value);
                                }else if(type.equals("float") || type.equals("Float")){
                                    arg = Float.parseFloat(value);
                                }else if(type.equals("double") || type.equals("Double")){
                                    arg = Double.parseDouble(value);
                                }else if(type.equals("long") || type.equals("Long")){
                                    arg = Long.parseLong(value);
                                }else if(type.equals("boolean") || type.equals("Boolean")){
                                    arg = Boolean.parseBoolean(value);
                                }else if(type.equals("String")){
                                    arg = value;
                                }else{
                                    continue;
                                }
                                // 再调用setter方法赋值
                                method.invoke(obj, arg);
                            }
                        }

                    }catch (Exception ex){
                    }
                }
            }
        }
    }
}
