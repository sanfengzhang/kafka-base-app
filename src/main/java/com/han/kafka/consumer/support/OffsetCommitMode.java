package com.han.kafka.consumer.support;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public enum OffsetCommitMode {

    /**不提交offset*/
    DISABLE,

    /**自己管理offset提交*/
    SELFCOMMITE,

    /**kafka自动提交offset*/
    AUTOCOMMIT;
}
