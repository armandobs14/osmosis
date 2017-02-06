package com.joint;

import java.util.List;
import wwwc.nees.joint.module.kao.AbstractKAO;

/**
 *
 * @author williams
 */
public class KAO extends AbstractKAO {

    public KAO() {
        super(null);
    }

    public List<Object[]> containsPoint(double lat, double lon) {
        StringBuilder query = new StringBuilder();
        query.append("prefix loc:<http://linkn.com.br/onto/locality/> ")
                .append("SELECT STR(?city) ?dataset ")
                .append("FROM <http://linkn.com.br/data/address/br/> ")
                .append("WHERE{?city loc:hasPolygon ?poly. ")
                .append("FILTER(bif:st_contains(bif:st_geomFromText(?poly), bif:st_point(" + lon + "," + lat + "))) ")
                .append("?city <dataset> ?dataset.} ")
                .append("LIMIT 1");
        return this.executeSPARQLqueryResultList(query.toString());
    }
}
