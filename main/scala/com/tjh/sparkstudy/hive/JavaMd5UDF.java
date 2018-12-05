package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;

/**
 * Created with IntelliJ IDEA.
 * User: tangjuhong
 * Date: 2018/9/13
 * Time: 上午11:22
 **/
public class JavaMd5UDF extends UDF {
    public String evaluate(String str) {
        String result="";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 计算md5函数
            md.update(str.getBytes());
            result=md.digest(str.getBytes("utf-8")).toString();
        }
        catch (Exception ex)
        {}

       return  result;
    }

    public static void main(String[] args)
    {
        JavaMd5UDF temp=new JavaMd5UDF();
        String result=  temp.evaluate("tangjuhong");
        System.out.println(result);
    }
}
