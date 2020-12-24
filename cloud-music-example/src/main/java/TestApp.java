import cn.hutool.core.codec.Base64;

import java.nio.charset.Charset;

/**
 * TODO:
 *
 * @author red
 * @class_name TestApp
 * @date 2020-10-24
 */
public class TestApp {

    public static void main(String[] args) {
        String json = "{\"one_to_one_status\":0,\"one_to_many_status\":1,\"tid\":8,\"id\":338602}";
        String encode = Base64.encode(json);
        System.out.println(encode);
        //
//        String base= new String(Base64.decode("eyJvbmVfdG9fb25lX3N0YXR1cyI6MCwib25lX3RvX21hbnlfc3RhdHVzIjoxLCJ0aWQiOjcsImlkIjozMzg2MDJ9"), Charset.defaultCharset());
//        System.out.println(base);
    }
}
