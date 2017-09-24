package com.ibeifeng.sparkproject.mapper.provider;

import com.ibeifeng.sparkproject.domain.AreaTop3;

import java.util.List;
import java.util.Map;

public class AreTopsMapperProvider {
    public String insertAll(Map map) {
        List<AreaTop3> users = (List<AreaTop3>) map.get("list");
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO area_top3_product (task_id,areas,area_level,product_id,city_names" +
                ",click_count,product_name,product_status) ");
        sb.append("VALUES");
        for (int i = 0; i < users.size(); i++) {
            AreaTop3 areaTop3 = users.get(i);
            if(i==users.size()-1) {
                sb.append("(");
                sb.append(areaTop3.getTaskid()).append(",");
                sb.append("\"").append(areaTop3.getArea()).append("\"").append(",");
                sb.append("\"").append(areaTop3.getAreaLevel()).append("\"").append(",");
                sb.append(areaTop3.getProductId()).append(",");
                sb.append("\"").append(areaTop3.getCityNames()).append("\"").append(",");
                sb.append(areaTop3.getCliclCount()).append(",");
                sb.append("\"").append(areaTop3.getProductName()).append("\"").append(",");
                sb.append(areaTop3.getProductStatus()).append(")");
            }else{
                sb.append("(");
                sb.append(areaTop3.getTaskid()).append(",");
                sb.append("\"").append(areaTop3.getArea()).append("\"").append(",");
                sb.append("\"").append(areaTop3.getAreaLevel()).append("\"").append(",");
                sb.append(areaTop3.getProductId()).append(",");
                sb.append("\"").append(areaTop3.getCityNames()).append("\"").append(",");
                sb.append(areaTop3.getCliclCount()).append(",");
                sb.append("\"").append(areaTop3.getProductName()).append("\"").append(",");
                sb.append(areaTop3.getProductStatus()).append(")").append(",");
            }
        }
        return  sb.toString();
    }
}
