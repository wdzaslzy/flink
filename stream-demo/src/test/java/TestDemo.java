import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDemo {

    public static void main(String[] args) throws ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parse = df.parse("2021-04-07 00:00:10");
        long time = parse.getTime();
        System.out.println(time);
    }

}
