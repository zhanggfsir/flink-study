package com.cub.entity;

/**
 * Description：Java中的实体类<br/>
 * Copyright (c) ，2020 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2020年02月28日
 *
 * @author 徐文波
 * @version : 1.0
 */
public class Student {
    /**
     * 学生名
     */
    private String name;

    /**
     * 考分
     */
    private double score;

    public Student() {
    }

    public Student(String name, double score) {
        this.name = name;
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    //  注意 不重写toSting类，打印的是地址
    @Override
    public String toString() {
        return "POJO类之Student实例的信息是=》学生名~" + name + "，考试分数~" + score;
    }
}
