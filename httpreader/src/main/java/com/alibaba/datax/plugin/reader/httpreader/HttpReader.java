package com.alibaba.datax.plugin.reader.httpreader;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httpreader.enums.ColumnEnum;
import com.alibaba.datax.plugin.reader.httpreader.enums.FormatEnum;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.datax.plugin.reader.httpreader.Constant.DEFAULT_COLUMN_TYPE;
import static com.alibaba.datax.plugin.reader.httpreader.Constant.HTTP_HEADER_CONTENT_TYPE;

/**
 * <p>
 * HttpReader
 * </p>
 *
 * @author JupiterMouse 2020/8/5
 * @since 1.0
 */
public class HttpReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            // 仅支持单个
            return Collections.singletonList(this.originConfig);
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.originConfig = this.getPluginJobConf();
            this.validateParameter();
            LOG.info("init() ok and end...");
        }

        @Override
        public void destroy() {
            LOG.debug("prepare() begin...");
        }

        private void validateParameter() {
            // 校验参数
            this.originConfig.getNecessaryValue(Key.URL, HttpReaderErrorCode.REQUIRED_VALUE);
            Map<String, Object> headers = this.originConfig.getMap(Key.HEADERS);
            if (!headers.containsKey(HTTP_HEADER_CONTENT_TYPE)) {
                throw DataXException
                        .asDataXException(
                                HttpReaderErrorCode.ILLEGAL_VALUE,
                                "请求头需要包含Content-Type");
            }
            String format = this.originConfig.getNecessaryValue(Key.FORMAT, HttpReaderErrorCode.REQUIRED_VALUE);
            Set<String> supportedFormats = Sets.newHashSet("JSON", "SOAP");
            if (!supportedFormats.contains(format)) {
                throw DataXException
                        .asDataXException(
                                HttpReaderErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 JSON,SOAP 两种模式, 不支持您配置的 format 格式 : [%s]",
                                        format));
            }
            String method = this.originConfig.getNecessaryValue(Key.METHOD, HttpReaderErrorCode.REQUIRED_VALUE);
            Set<String> supportedMethods = Sets.newHashSet("GET", "POST");
            if (!supportedMethods.contains(method)) {
                throw DataXException
                        .asDataXException(
                                HttpReaderErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 GET,POST 两种模式, 不支持您配置的 method 格式 : [%s]",
                                        method));
            }
            this.originConfig.getNecessaryValue(Key.MAP_PATH, HttpReaderErrorCode.REQUIRED_VALUE);
        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        //配置文件
        private Configuration readerSliceConfig;

        private String format = null;

        private String mapPath = null;

        List<ColumnEntry> httpColumnMeta = null;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.format = this.readerSliceConfig.getString(Key.FORMAT);
            this.mapPath = this.readerSliceConfig.getString(Key.MAP_PATH);
            String cloumsStr = readerSliceConfig.getString(Key.HTTP_COLUMN);
            if (cloumsStr.startsWith(Constant.COLUMN_ENTRY_PATTERN)) {
                this.httpColumnMeta = JSON.parseArray(cloumsStr, ColumnEntry.class);
            } else if (cloumsStr.startsWith(Constant.COLUMN_ARRAY_PATTERN)) {
                List<String> list = readerSliceConfig.getList(Key.HTTP_COLUMN, String.class);
                httpColumnMeta = new ArrayList<>();
                list.forEach(name -> httpColumnMeta.add(new ColumnEntry(name)));
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            HttpReaderClientUtil httpReaderClientUtil = new HttpReaderClientUtil(this.readerSliceConfig);
            String response = httpReaderClientUtil.sendRequest();
            JSONObject jsonObject = null;
            if (FormatEnum.SOAP.name().equalsIgnoreCase(this.format)) {
                jsonObject = XML.toJSONObject(response);
            } else if (FormatEnum.JSON.name().equalsIgnoreCase(this.format)) {
                jsonObject = new JSONObject(response);
            }
            String[] paths = this.mapPath.split(",");
            JSONArray jsonArray = null;
            if (paths.length >= 1) {
                int i = 1;
                for (; i < paths.length; i++) {
                    assert jsonObject != null;
                    jsonObject = jsonObject.getJSONObject(paths[i - 1]);
                }
                assert jsonObject != null;
                jsonArray = jsonObject.getJSONArray(paths[i - 1]);
            } else {
                assert jsonObject != null;
                jsonObject.getJSONArray("");
            }
            assert jsonArray != null;
            if (jsonArray.isEmpty()) {
                LOG.info("源数据为空");
                return;
            }
            LOG.debug("============================> httpColumnMeta start \n");
            LOG.debug(JSON.toJSONString(this.httpColumnMeta));
            LOG.debug("============================> httpColumnMeta end \n");
            for (Object o : jsonArray) {
                Map<String, Object> item = ((JSONObject) o).toMap();
                LOG.debug("================================> line data start\n");
                LOG.debug(JSON.toJSONString(item));
                LOG.debug("================================> line data end\n");
                recordSender.sendToWriter(this.transportOneRecord(recordSender, this.httpColumnMeta, item,
                        this.getTaskPluginCollector()));
            }
            LOG.info("httpReader is completed");
        }

        @Override
        public void destroy() {
        }

        private Record transportOneRecord(RecordSender recordSender,
                                          List<ColumnEntry> httpColumnMeta, Map<String, Object> sourceMap,
                                          TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();
            Column columnGenerated = null;
            // 目前只支持指定value的映射
            try {
                for (ColumnEntry columnEntry : httpColumnMeta) {
                    Object tempCol = sourceMap.get(columnEntry.getName());
                    // 默认
                    String columnType = Optional.ofNullable(columnEntry.getType()).orElse(DEFAULT_COLUMN_TYPE);
                    ColumnEnum type = ColumnEnum.valueOf(columnType.toUpperCase());
                    LOG.debug("============================> field start \n");
                    LOG.debug("tempCol:{},columnType:{},FieldType:{}", tempCol, columnType, type.name());
                    LOG.debug("============================> field end \n");
                    if (tempCol == null) {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }
                    // 类型转换
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(tempCol.toString());
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(tempCol.toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", tempCol,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(tempCol.toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", tempCol,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(tempCol.toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", tempCol,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                String formatString = columnEntry.getFormat();
                                if (StringUtils.isNotBlank(formatString)) {
                                    // 用户自己配置的格式转换, 脏数据行为出现变化
                                    DateFormat format = columnEntry
                                            .getDateFormat();
                                    columnGenerated = new DateColumn(
                                            format.parse(tempCol.toString()));
                                } else {
                                    // 框架尝试转换
                                    columnGenerated = new DateColumn(
                                            new StringColumn(tempCol.toString()).asDate());
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", tempCol,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw DataXException
                                    .asDataXException(
                                            HttpReaderErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }
                    LOG.debug("============================> columnGenerated start \n");
                    LOG.debug(JSON.toJSONString(columnGenerated));
                    LOG.debug("============================> columnGenerated end \n");
                    record.addColumn(columnGenerated);
                }
            } catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
                taskPluginCollector
                        .collectDirtyRecord(record, iae.getMessage());
            } catch (Exception e) {
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
                // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
            return record;
        }
    }
}
