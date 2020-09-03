package cn.bmsoft.flink;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.util.Collector;

/**
 * Created by yml on 2019/10/28.
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
    final     ParameterTool params = ParameterTool.fromArgs(args);
    final     ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataSource<String> dataSource = env.fromElements(WORDS);
        dataSource.flatMap(new RichFlatMapFunction<String, Tuple2<String,Integer>>() {
            //创建一个累加器
        private     IntCounter linesNum = new IntCounter();

            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split("\\W+");
                for (String split : splits) {
                    if (split.length()>0){
                      collector.collect(new Tuple2<String, Integer>(split,1));
                    }
                }
                linesNum.add(1);

            }
            @Override
            public void open(Configuration parameters) throws Exception {
                //注册一个累加器
                getRuntimeContext().addAccumulator("linesNum", linesNum);

            }

        }).groupBy(0).sum(1).print();
        int linesNum = env.getLastJobExecutionResult().getAccumulatorResult("linesNum");
        System.out.println(linesNum+"==========================");
    }
    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come,",
            "When we have shuffled off this mortal coil,",
            "Must give us pause: there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would these fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };

}
