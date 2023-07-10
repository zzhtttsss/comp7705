package org.comp7705.client.utils;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ProgressBar {


    /**
     * current progress
     */
    private int index;
    private int step;
    /**
     * progress bar length
     */
    private final int barLength;

    private boolean hasInited = false;
    private boolean hasFinished = false;
    private String title;

    private static final char processChar = '█';
    private static final char waitChar = '─';


    private ProgressBar() {
        index = 0;
        step = 1;
        barLength = 100;
        title = "Progress:";
    }

    public static ProgressBar build() {
        return new ProgressBar();
    }

    public static ProgressBar build(int step) {
        ProgressBar progressBar = build();
        progressBar.step = step;
        return progressBar;
    }

    public static ProgressBar build(int index, int step) {
        ProgressBar progressBar = build(step);
        progressBar.index = index;
        return progressBar;
    }

    public static ProgressBar build(int index, int step, String title) {
        ProgressBar progressBar = build(index, step);
        progressBar.title = title;
        return progressBar;
    }

    private String generate(int num, char ch) {
        if (num == 0) return "";
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < num; i++) {
            builder.append(ch);
        }
        return builder.toString();
    }

    private String genProcess(int num) {
        return generate(num, processChar);
    }

    private String genWaitProcess(int num) {
        return generate(num, waitChar);
    }


    private void cleanProcessBar() {
        System.out.print(generate(barLength / step + 6, '\b'));
    }


    public void process() {
        checkStatus();
        checkInit();
        cleanProcessBar();
        index++;
        drawProgressBar();
        checkFinish();
    }

    public void process(int process) {
        checkStatus();
        checkInit();
        cleanProcessBar();
        if (index + process >= barLength)
            index = barLength;
        else
            index += process;
        drawProgressBar();
        checkFinish();
    }

    public void step() {
        checkStatus();
        checkInit();
        cleanProcessBar();
        if (index + step >= barLength)
            index = barLength;
        else
            index += step;
        drawProgressBar();
        checkFinish();
    }


    public void drawProgressBar() {
        System.out.printf(
                "%3d%%├%s%s┤",
                index,
                genProcess(index / step),
                genWaitProcess(barLength / step - index / step)
        );
    }


    private void checkStatus() {
        if (hasFinished) throw new IllegalStateException("ProgressBar has been completed!");
    }

    private void checkInit() {
        if (!hasInited) init();
    }


    private void checkFinish() {
        if (hasFinished() && !hasFinished) finish();
    }

    public boolean hasFinished() {
        return index >= barLength;
    }

    private void init() {
        checkStatus();
        System.out.print(title);
        System.out.printf("%3d%%[%s%s]", index, genProcess(index / step), genWaitProcess(barLength / step - index / step));
        hasInited = true;
    }

    private void finish() {
        System.out.println();
        hasFinished = true;
    }

    public void printProgress() throws InterruptedException {
        init();
        do {
            step();
            Thread.sleep(50);
            index++;
        } while (index <= barLength);
        System.out.println();
    }
}
