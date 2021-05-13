package io.github.xlblog.file;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonInputFormat extends DelimitedInputFormat<TestInterface> {

    public JsonInputFormat(String path) {
        super(new Path(path), null);
    }

    @Override
    public TestInterface readRecord(TestInterface reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        if (this.getDelimiter() != null && this.getDelimiter().length == 1 && this.getDelimiter()[0] == 10 && offset + numBytes >= 1 && bytes[offset + numBytes - 1] == 13) {
            --numBytes;
        }

        String jsonStr = new String(bytes, offset, numBytes, StandardCharsets.UTF_8);
        return JSON.parseObject(jsonStr, UserBean.class);
    }

    @Override
    public boolean reachedEnd() {
        return super.reachedEnd();
    }
}
