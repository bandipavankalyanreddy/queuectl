package com.queuectl.worker;

import com.queuectl.core.Job;
import com.queuectl.storage.JobStore;
import java.io.IOException;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

/** Runs worker threads in the foreground and uses PID files for another CLI to stop them. */
public final class WorkerManager {
    public static final long LEASE_MILLIS = 20_000;
    private static final long REAPER_INTERVAL_MILLIS = 5_000;
    private WorkerManager() { }
    public static void start(String[] args, JobStore store, Path queueDir) throws Exception {
        if (args.length != 4 || !args[2].equals("--count")) throw new IllegalArgumentException("worker start requires --count N");
        int count = Integer.parseInt(args[3]); if(count<1) throw new IllegalArgumentException("count must be positive");
        Path workers=queueDir.resolve("workers"); Files.createDirectories(workers);
        long pid=ProcessHandle.current().pid(); Path pidFile=workers.resolve(pid+".pid"); Files.writeString(pidFile,Long.toString(pid),StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { Files.deleteIfExists(pidFile); } catch(IOException ignored) {} }));
        Worker[] all=new Worker[count]; List<Thread> threads=new ArrayList<>();
        for(int i=0;i<count;i++){all[i]=new Worker(""+pid+"-"+i,store); Thread t=new Thread(all[i],"queuectl-worker-"+i);threads.add(t);t.start();}
        System.err.println("started "+count+" worker thread(s), pid "+pid);
        try { for(Thread t:threads)t.join(); } finally { Files.deleteIfExists(pidFile); }
    }
    public static void stop(Path queueDir) throws Exception {
        Path workers=queueDir.resolve("workers"); if(!Files.isDirectory(workers)){System.out.println("no workers");return;}
        try(var paths=Files.list(workers)){for(Path f:(Iterable<Path>)paths::iterator){try{long pid=Long.parseLong(Files.readString(f).trim());ProcessHandle.of(pid).ifPresent(ProcessHandle::destroy);}catch(Exception e){Files.deleteIfExists(f);}}}
        long end=System.nanoTime()+Duration.ofSeconds(10).toNanos(); while(System.nanoTime()<end){try(var p=Files.list(workers)){if(p.findAny().isEmpty()){System.out.println("workers stopped");return;}}Thread.sleep(100);}
        System.err.println("timed out waiting for worker PID files");
    }
    private static final class Worker implements Runnable {
        private final String id; private final JobStore store; private volatile boolean shuttingDown;
        Worker(String id,JobStore store){this.id=id;this.store=store;Runtime.getRuntime().addShutdownHook(new Thread(()->shuttingDown=true));}
        public void run() { long lastReap=0; while(!shuttingDown){try { long now=System.currentTimeMillis(); if(now-lastReap>=REAPER_INTERVAL_MILLIS){store.reapExpired(now);lastReap=now;} int maxRetries=store.configInt("max-retries"); var maybe=store.claimNext(id,now+LEASE_MILLIS,now,maxRetries); if(maybe.isEmpty()){Thread.sleep(150);continue;} execute(maybe.get()); } catch(Exception e){System.err.println("worker "+id+": "+e.getMessage()); try{Thread.sleep(250);}catch(InterruptedException ignored){}}} }
        private void execute(Job job) throws SQLException { try { Process process=new ProcessBuilder(shellCommand(job.command())).inheritIO().start();
            while (!process.waitFor(2, TimeUnit.SECONDS)) store.renewLease(job.id(), id, System.currentTimeMillis()+LEASE_MILLIS);
            if(process.exitValue()==0)store.complete(job.id()); else failure(job);
        } catch(InterruptedException e){Thread.currentThread().interrupt(); failure(job); } catch(IOException e){failure(job); } }
        private void failure(Job job) throws SQLException { int attempts=job.attempts()+1; int base=store.configInt("backoff-base"); int max=store.configInt("max-retries"); long delaySeconds=(long)Math.pow(base,attempts); store.fail(job.id(),attempts,max,System.currentTimeMillis()+delaySeconds*1000); }
        private static String[] shellCommand(String command) { boolean windows=System.getProperty("os.name").toLowerCase().contains("win"); return windows?new String[]{"cmd","/c",command}:new String[]{"sh","-c",command}; }
    }
}
