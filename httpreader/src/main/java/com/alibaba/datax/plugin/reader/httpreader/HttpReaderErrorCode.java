package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * <p>
 * 错误信息
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public enum HttpReaderErrorCode implements ErrorCode {
    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE", "参数不合法"),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION", "未知异常"),
    REQUIRED_VALUE("REQUIRED_VALUE", "您缺失了必须填写的参数值."),
    NOT_SUPPORT_TYPE("NOT_SUPPORT_TYPE", "类型不支持");

    private final String code;
    private final String description;

    HttpReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
