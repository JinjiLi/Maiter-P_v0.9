/* 
 * File:   table-registry.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef TABLE_REGISTRY_H
#define	TABLE_REGISTRY_H
#include "../util/common.h"
#include "table.h"
#include "statetable.h"

namespace dsm {


    // method for create statetable

    template<class K, class V1, class V2, class V3,class E>
    static StateTable<K, V1, V2, V3,E>* CreateTable(double schedule_portion,
            Sharder<K>* sharder,
            IterateKernel<K, V1, V3,E>* iterkernel,
            TermChecker<K, V2>* termchecker) {
        TableDescriptor *info = new TableDescriptor();
        info->key_marshal = new Marshal<K>;
        info->value1_marshal = new Marshal<V1>;
        info->value2_marshal = new Marshal<V2>;
        info->value3_marshal = new Marshal<V3>;
        info->sharder = sharder;
        info->iterkernel = iterkernel;
        info->termchecker = termchecker;
        info->partition_factory = new typename StateTable<K, V1, V2, V3,E>::Factory;
        info->schedule_portion = schedule_portion;

        return CreateTable<K, V1, V2, V3,E>(info);
    }

    template<class K, class V1, class V2, class V3,class E>
    static StateTable<K, V1, V2, V3,E>* CreateTable(const TableDescriptor *info) {
        StateTable<K, V1, V2, V3,E> *t = (StateTable<K,V1,V2,V3,E>*)info->partition_factory->New();
        t->Init(info);
        return t;
    }

}


#endif	/* TABLE_REGISTRY_H */

