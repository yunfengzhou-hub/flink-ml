package org.apache.flink.ml.common.function.tomcat;

import org.apache.flink.ml.common.function.StreamFunction;
import org.apache.flink.ml.common.utils.PipelineUtils;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@WebServlet(urlPatterns = "/", loadOnStartup = 0)
public class FlinkInferenceServlet extends HttpServlet {
    StreamFunction<MenuItem, MenuItem> function;

    public FlinkInferenceServlet() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("/tmp/model.txt"));

        String functionString = br.readLine();

        function = PipelineUtils.deserializeFunction(functionString);
    }

    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("text/html");

        String name = req.getParameter("name");
        if (name == null) {
            name = "burger";
        }

        String priceString = req.getParameter("price");
        if (priceString == null) {
            priceString = "9.99";
        }
        Double price = Double.valueOf(priceString);

        MenuItem inputData = new MenuItem(name, price);

        List<MenuItem> outputDataList = function.apply(inputData);

        PrintWriter pw = resp.getWriter();
        for(MenuItem outputData:outputDataList){
            pw.write(outputData.toString());
        }
        pw.flush();
    }
}
