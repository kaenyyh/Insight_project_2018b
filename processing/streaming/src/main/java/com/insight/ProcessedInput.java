package com.insight;

public class ProcessedInput {
    public String articleTitle;
    public String userName;
    public String time;

    public ProcessedInput(String articleTitle, String userName, String time) {
        this.articleTitle = articleTitle;
        this.userName = userName;
        this.time = time;
    }

    public String getArticleTitle() {
        return articleTitle;
    }

    public String getTime() {
        return time;
    }

    public String getUserName() {
        return userName;
    }
}
