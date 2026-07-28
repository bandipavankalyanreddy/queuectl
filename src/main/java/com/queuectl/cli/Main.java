package com.queuectl.cli;

import com.queuectl.core.Job;
import com.queuectl.storage.Database;
import com.queuectl.storage.JobStore;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Small, dependency-free command dispatcher. Operational messages go to stderr. */
public final class Main {
    private static final Path QUEUE_DIR = Path.of(".queuectl");
    public static void main(String[] args) {
        try { run(args, new JobStore(new Database(QUEUE_DIR))); }
        catch (Exception e) { System.err.println("queuectl: " + e.getMessage()); System.exit(1); }
    }
    private static void run(String[] a, JobStore store) throws Exception {
        if (a.length == 0) { usage(); return; }
        if (a[0].equals("enqueue") && a.length == 2) { enqueue(a[1], store); return; }
        if (a[0].equals("list")) { list(a, store); return; }
        if (a[0].equals("clear") && a.length == 1) { clear(store); return; }
        if (a[0].equals("status")) { status(store); return; }
        if (a[0].equals("dlq") && a.length >= 2 && a[1].equals("list")) { printJobs(store.list("dead"), false); return; }
        if (a[0].equals("dlq") && a.length == 3 && a[1].equals("retry")) { store.retryDead(a[2]); System.out.println("retried " + a[2]); return; }
        if (a[0].equals("config") && a.length == 4 && a[1].equals("set")) { configSet(a[2], a[3], store); return; }
        if (a[0].equals("worker") && a.length >= 2 && a[1].equals("start")) { worker("start", a, store); return; }
        if (a[0].equals("worker") && a.length == 2 && a[1].equals("stop")) { worker("stop", a, store); return; }
        usage(); throw new IllegalArgumentException("invalid command");
    }
    private static void enqueue(String json, JobStore store) throws SQLException {
        String id = jsonField(json, "id"), command = jsonField(json, "command");
        if (id == null || command == null) throw new IllegalArgumentException("enqueue JSON needs id and command strings");
        int max = 3; // Config is read when a worker claims; this is retained as an audit snapshot.
        store.enqueue(id, command, max); System.out.println("enqueued " + id);
    }
    private static void list(String[] args, JobStore store) throws SQLException {
        String state = null; boolean json = false;
        for (int i=1;i<args.length;i++) { if (args[i].equals("--state") && i+1<args.length) state=args[++i]; else if(args[i].equals("--json")) json=true; else throw new IllegalArgumentException("invalid list option"); }
        printJobs(store.list(state), json);
    }
    private static void status(JobStore s) throws SQLException { for(String state:List.of("pending","processing","failed","completed","dead")) System.out.println(state + ": " + s.count(state)); }
    private static void clear(JobStore store) throws SQLException {
        store.clearAll();
        System.out.println("cleared all jobs");
    }
    private static void configSet(String key, String value, JobStore store) {
        if (!key.equals("max-retries") && !key.equals("backoff-base")) throw new IllegalArgumentException("unknown config key");
        try { int n=Integer.parseInt(value); if(n<1) throw new IllegalArgumentException("config value must be positive");
            try(var c=new Database(QUEUE_DIR).connect(); var p=c.prepareStatement("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")){p.setString(1,key);p.setString(2,value);p.executeUpdate();}
        } catch (SQLException e) { throw new IllegalStateException(e); }
        System.out.println("set " + key + "=" + value);
    }
    private static void worker(String action, String[] args, JobStore store) throws Exception {
        Class<?> type=Class.forName("com.queuectl.worker.WorkerManager");
        if(action.equals("start")) type.getMethod("start",String[].class,JobStore.class,Path.class).invoke(null,args,store,QUEUE_DIR);
        else type.getMethod("stop",Path.class).invoke(null,QUEUE_DIR);
    }
    private static String jsonField(String json, String field) { Matcher m=Pattern.compile("\\\""+field+"\\\"\\s*:\\s*\\\"((?:\\\\.|[^\\\"])*)\\\"").matcher(json); return m.find()?m.group(1).replace("\\\"","\"").replace("\\\\","\\"):null; }
    private static void printJobs(List<Job> jobs, boolean json) { if(json){System.out.println("["+jobs.stream().map(Main::asJson).reduce((x,y)->x+","+y).orElse("")+"]");}else jobs.forEach(j->System.out.println(j.id()+" "+j.state()+" attempts="+j.attempts()+" command="+j.command())); }
    private static String asJson(Job j) { return "{\"id\":\""+escape(j.id())+"\",\"command\":\""+escape(j.command())+"\",\"state\":\""+j.state()+"\",\"attempts\":"+j.attempts()+",\"max_retries\":"+j.maxRetries()+"}"; }
    private static String escape(String s) { return s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n"); }
    private static void usage() { System.err.println("usage: queuectl enqueue JSON | worker start --count N | worker stop | status | clear | list --state STATE [--json] | dlq list|retry ID | config set KEY VALUE"); }
}
