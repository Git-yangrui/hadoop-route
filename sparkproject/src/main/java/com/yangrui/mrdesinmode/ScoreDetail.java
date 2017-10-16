package com.yangrui.mrdesinmode;

import java.io.Serializable;

public class ScoreDetail implements Serializable {
    //case class ScoreDetail(studentName: String, subject: String, score: Float)
    public String studentName;
    public String subject;
    public float score;

    public ScoreDetail(String studentName, String subject, float score) {
        this.studentName = studentName;
        this.subject = subject;
        this.score = score;
    }




}