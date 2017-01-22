package io.jacob.etlman.metastore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoy on 1/4/2017.
 */
public class ETLColumnMapping {
    private ETLEntityAttribute entityAttribute;
    private List<ETLSourceColumn> srcColumnList = new ArrayList<ETLSourceColumn>();
    private String expression;
    private String comments;

    public String toString() {

        StringBuilder buffer = new StringBuilder();

        buffer.append("Mapping for ")
                .append(entityAttribute.getSysName()).append(".")
                .append(entityAttribute.getSchemaName()).append(".")
                .append(entityAttribute.getTableName()).append(".")
                .append(entityAttribute.getColumnName()).append(": ");

        int i = 0;
        for (ETLSourceColumn sourceColumn : srcColumnList) {
            if (i > 0)
                buffer.append(",");
            buffer.append(sourceColumn.getSysName()).append(".")
                    .append(sourceColumn.getSchemaName()).append(".")
                    .append(sourceColumn.getTableName()).append(".")
                    .append(sourceColumn.getColumnName());
            i++;
        }

        buffer.append("\n With Expression:").append(expression).append("\n");

        return buffer.toString();
    }

    public ETLEntityAttribute getEntityAttribute() {
        return entityAttribute;
    }

    public void setEntityAttribute(ETLEntityAttribute entityAttribute) {
        this.entityAttribute = entityAttribute;
    }

    public List<ETLSourceColumn> getSrcColumnList() {
        return srcColumnList;
    }

    public void setSrcColumnList(List<ETLSourceColumn> srcColumnList) {
        this.srcColumnList = srcColumnList;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
}
