package com.allen.demo.response;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * REST 结构化输出结果对象
 *
 * @param <T>
 * @author qisong.li@bitmain.com
 * @version 1.0.0
 * @since 2018/11/23
 */
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 8992436576262574064L;

    @JSONField(name = "Code")
    @JsonProperty(value = "Code")
    private Integer code;

    @JSONField(name = "Msg")
    @JsonProperty(value = "Msg")
    private String msg;

    @JSONField(name = "Result")
    @JsonProperty(value = "Result")
    private T result;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public Result() {

    }

    public Result<T> code(Integer code) {
        this.code = code;
        return this;
    }

    public Result<T> msg(String msg) {
        this.msg = msg;
        return this;
    }

    public Result<T> result(T result) {
        this.result = result;
        return this;
    }

    /**
     * 请求成功默认返回0
     *
     * @return
     */
    public static Result ok() {
        return new Result()
                .code(ErrorCode.RESULT_SUCCESS.getCode())
                .msg(ErrorCode.RESULT_SUCCESS.getMsg());
    }

    /**
     * 请求成功返回0+结果对象
     *
     * @param result
     * @param <T>
     * @return
     */
    public static <T> Result<T> ok(T result) {
        return new Result<T>()
                .code(ErrorCode.RESULT_SUCCESS.getCode())
                .msg(ErrorCode.RESULT_SUCCESS.getMsg())
                .result(result);
    }

    /**
     * 全部自定义消息 与错误码
     *
     * @param code
     * @param message
     * @param <T>
     * @return
     */
    public static <T> Result<T> error(Integer code, String message) {
        Result<T> msg = new Result<>();
        msg.msg = message;
        msg.code = code;
        return msg;
    }

    /**
     * 传入对应的错误码 自动返回对应错误消息
     *
     * @param code
     * @param <T>
     * @return
     */
    public static <T> Result<T> error(Integer code) {
        return error(code, ErrorCode.getMsg(code));
    }

    /**
     * 使用系统默认错误码 传入错误返回结果
     *
     * @param result
     * @param <T>
     * @return
     */
    public static <T> Result<T> errorResult(T result) {
        return new Result<T>()
                .result(result)
                .code(ErrorCode.BUSINESS_FAIL.getCode()).msg(ErrorCode.BUSINESS_FAIL.getMsg());
    }

    /**
     * 系统定义的错误返回结果
     *
     * @param <T>
     * @return
     */
    public static <T> Result<T> error() {
        return error(ErrorCode.BUSINESS_FAIL.getCode(), ErrorCode.BUSINESS_FAIL.getMsg());
    }

    /**
     * 自定义返回错误结果 默认错误码为1 业务处理失败
     *
     * @param message
     * @param <T>
     * @return
     */
    public static <T> Result<T> error(String message) {
        return error(ErrorCode.BUSINESS_FAIL.getCode(), message);
    }
}
