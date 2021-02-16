package com.qf.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Description：Java中的中学生实体类<br/>
 * Copyright (c) ，2020 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2020年02月28日
 *
 * @author 徐文波
 * @version : 1.0
 */
@Data
//@AllArgsConstructor
@NoArgsConstructor
public class MiddleStudent {
    /**
     * 学生名
     */
    private String name;

    /**
     * 考分
     */
    private double score;

    public MiddleStudent(String name, double score) {
        this.name = name;
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public double getScore() {
        return score;
    }
}
