package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.AreaTop3;
import com.ibeifeng.sparkproject.mapper.provider.AreTopsMapperProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface AreTopsMapper {

    @InsertProvider(type = AreTopsMapperProvider.class, method = "insertAll")
    void insertAll(@Param("list") List<AreaTop3> areaTop3s);
}
