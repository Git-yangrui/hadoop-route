package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.AreaTop3;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:application-core.xml"})
public class AreTopsMapperTest {
    @Autowired
    AreTopsMapper areTopsMapper;

    @Test
    public void test_batchInsert(){
        List<AreaTop3> lis=new ArrayList<>();
        for(int i=0;i<10;i++){
            AreaTop3 areaTop3=new AreaTop3();
            areaTop3.setTaskid(1111l+i);
            areaTop3.setCityNames("cityname");
            areaTop3.setArea("area");
            areaTop3.setAreaLevel("arealevel");
            areaTop3.setCliclCount(10+i);
            areaTop3.setProductId(100);
            areaTop3.setProductName("sdsds");
            areaTop3.setProductStatus("1");
            lis.add(areaTop3);
        }

        areTopsMapper.insertAll(lis);

    }

}
