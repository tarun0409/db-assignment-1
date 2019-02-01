import sys
import sqlparse

def parse_condition(condition_list):
    curr_cond = ''
    condition = dict()
    operands = list()
    for cond in condition_list:
        if cond == 'AND' or cond == 'OR' or cond=='and' or cond=='or':
            condition['operator'] = cond
            operands.append(curr_cond)
            curr_cond = ''
            continue
        curr_cond += str(cond)
    operands.append(curr_cond)
    operand_dict_list = list()
    operators = ['=','>','<','>=','<=']
    for operand in operands:
        for op in operators:
            if op in operand:
                operand_dict = dict()
                op_split = operand.split(op)
                operand_dict['operator'] = op
                operand_dict['operands'] = op_split
                operand_dict_list.append(operand_dict)
    condition['operands'] = operand_dict_list
    return condition


def parse_query(query):
    tokens = query.split(' ')
    query_dict = dict()
    for i in range(0,len(tokens)):
        if tokens[i]=='select':
            for j in range((i+1),len(tokens)):
                if tokens[j]=='from':
                    break
                if 'columns' not in query_dict:
                    query_dict['columns'] = list()
                cols = tokens[j].split(',')
                for col in cols:
                    if col=='' or col==' ':
                        continue
                    query_dict['columns'].append(col)
        elif tokens[i]=='from':
            for j in range((i+1),len(tokens)):
                if tokens[j]=='where':
                    break
                if 'tables' not in query_dict:
                    query_dict['tables'] = list()
                tabs = tokens[j].split(',')
                for tab in tabs:
                    if tab=='' or tab==' ':
                        continue
                    if tab[len(tab)-1]==';':
                        tab = tab[:len(tab)-1]
                    query_dict['tables'].append(tab)
        elif tokens[i]=='where':
            condition = list()
            for j in range((i+1),len(tokens)):
                if tokens[j]==';':
                    break
                tok = tokens[j]
                if tok[len(tok)-1]==';':
                    tok = tok[:len(tok)-1]
                condition.append(tok)
            query_dict['condition'] = parse_condition(condition)
    return query_dict

def parse_metadata(file_path):
    table = dict()
    line_one = False
    curr_table_name = None
    with open(file_path) as f:
        for line in f:
            token = line.strip()
            if token == '<begin_table>':
                line_one = True
            elif token == '<end_table>':
                continue
            elif line_one:
                table[token] = list()
                curr_table_name = token
                line_one = False
            else:
                table[curr_table_name].append(token)
    return table

def satisfy_operator(optr, opnd1, opnd2):
    if optr == '=':
        return opnd1 == opnd2
    elif optr == '>':
        return opnd1 > opnd2
    elif optr == '<':
        return opnd1 < opnd2
    elif optr == '>=':
        return opnd1 >= opnd2
    elif optr == '<=':
        return opnd1 <= opnd2
    return False

query_str = sys.argv[1].strip()
if query_str[len(query_str)-1] != ';':
    print
    print 'Syntax Error: Expected ; in the end of query!'
    print
    sys.exit()
query_obj = parse_query(query_str)
select_cols = query_obj['columns']
metadata = parse_metadata('metadata.txt')
qt = query_obj['tables']
if len(qt)==1:
    table_name = qt[0]
    attr_list = list()
    for x in metadata[table_name]:
        if '.' not in x:
            attr_list.append(table_name+'.'+x)
        else:
            attr_list.append(x)
    if '*' in select_cols:
        select_cols = attr_list[:]
    else:
        temp_cols = list()
        for x in select_cols:
            if '.' not in x:
                temp_cols.append(table_name+'.'+x)
            else:
                temp_cols.append(x)
        select_cols = temp_cols
    select_indices = list()
    mod_attr_list = list()
    for i in range(0,len(attr_list)):
        if attr_list[i] in select_cols:
            mod_attr_list.append(attr_list[i])
            select_indices.append(i)

    print ",".join(mod_attr_list)
    
    table_file = table_name+'.csv'
    condition = None
    cond_ops = None
    if 'condition' in query_obj:
        condition = query_obj['condition']
        cond_ops = condition['operands']
    and_op = False
    if condition is not None and 'operator' in condition:
        log_op = condition['operator']
        if log_op == 'AND' or log_op == 'and':
            and_op = True
            
    with open(table_file) as t:
        for row_string in t:
            row_split = row_string.split(',')
            mod_row_split = list()
            for i in range(0,len(row_split)):
                if i in select_indices:
                    mod_row_split.append(row_split[i].strip())
            mod_row_str = ",".join(mod_row_split)
            if condition is None:
                print mod_row_str
            else:
                row_split = row_string.split(',')
                row_approved = True
                total_conditions = len(cond_ops)
                satisfied_conditions = 0    
                for i in range(0,len(row_split)):
                    key = metadata[table_name][i]
                    value = row_split[i]
                    for cond_op in cond_ops:
                        if cond_op['operands'][0] == key:
                            optr = cond_op['operator']
                            opnd = cond_op['operands'][1]
                            if satisfy_operator(optr, int(value), int(opnd)):
                                satisfied_conditions += 1
                            elif and_op:
                                row_approved = False
                                break
                    if not row_approved:
                        break
                if and_op and satisfied_conditions != total_conditions:
                    row_approved = False
                elif not and_op and satisfied_conditions<=0:
                    row_approved = False
                if row_approved:
                    print mod_row_str

else:
    table_name1 = qt[0]
    table_name2 = qt[1]
    table_file1 = table_name1+'.csv'
    table_file2 = table_name2+'.csv'
    attr_list1 = list()
    attr_list2 = list()
    for x in metadata[table_name1]:
        if '.' not in x:
            attr_list1.append(table_name1+'.'+x)
        else:
            attr_list1.append(x)
    for x in metadata[table_name2]:
        if '.' not in x:
            attr_list2.append(table_name2+'.'+x)
        else:
            attr_list2.append(x)

    attr_list = list()
    for attr in attr_list1:
        attr_list.append(attr)
    for attr in attr_list2:
        attr_list.append(attr)


    if '*' in select_cols:
        select_cols = attr_list[:]
    else:
        temp_cols = list()
        for x in select_cols:
            if '.' not in x:
                if x in metadata[table_name1]:
                    temp_cols.append(table_name1+'.'+x)
                elif x in metadata[table_name2]:
                    temp_cols.append(table_name2+'.'+x)
                else:
                    print
                    print x+' Column does not exist'
                    print
            else:
                temp_cols.append(x)
        select_cols = temp_cols
    select_indices = list()
    mod_attr_list = list()
    for i in range(0,len(attr_list)):
        if attr_list[i] in select_cols:
            select_indices.append(i)
            mod_attr_list.append(attr_list[i])
    print ",".join(mod_attr_list)
         

    condition = None
    cond_ops = None
    if 'condition' in query_obj:
        condition = query_obj['condition']
        cond_ops = condition['operands']
    if cond_ops is not None:
        try_val = cond_ops[0]['operands'][1]
        try:
            temp_val = int(try_val)
            and_op = False
            if 'operator' in condition:
                log_op = condition['operator']
                if log_op == 'AND' or log_op == 'and':
                    and_op = True
    
            with open(table_file1) as t1:
                for row_string1 in t1:
                    with open(table_file2) as t2:
                        for row_string2 in t2:
                            row_string = row_string1.strip()+','+row_string2.strip()
                            val_list = row_string.split(',')
                            total_conditions = len(cond_ops)
                            satisfied_conditions = 0
                            row_approved = True
                            for cond in cond_ops:
                                opnd1 = cond['operands'][0]
                                opnd2 = int(cond['operands'][1])
                                optr = cond['operator']
                                col_index = -1
                                for i in range(0,len(attr_list)):
                                    if opnd1 == attr_list[i]:
                                        col_index = i
                                        break
                                    elif opnd1 == attr_list[i].split('.')[1]:
                                        col_index = i
                                        break
                                val = int(val_list[col_index])
                                if satisfy_operator(optr, val, opnd2):
                                    satisfied_conditions += 1
                                elif and_op:
                                    row_approved = False
                                    break
                            if not and_op and satisfied_conditions<=0:
                                row_approved = False
                            if row_approved:
                                # print row_string
                                row_string = row_string1.strip()+','+row_string2.strip()
                                row_list = row_string.split(',')
                                mod_row_list = list()
                                for i in range(0,len(row_list)):
                                    if i in select_indices:
                                        mod_row_list.append(row_list[i])
                                print ",".join(mod_row_list)
            
        except ValueError:
            #join
            col1 = cond_ops[0]['operands'][0]
            col2 = cond_ops[0]['operands'][1]
            col1_index = -1
            col2_index = -1
            mod_attr_list = list()
            for i in range(0,len(attr_list)):
                if col1 == attr_list[i]:
                    col1_index = i
                    mod_attr_list.append(attr_list[i])
                elif col2 == attr_list[i]:
                    col2_index = i
                else:
                    mod_attr_list.append(attr_list[i])
            print ",".join(mod_attr_list)
            with open(table_file1) as t1:
                for row_string1 in t1:
                    with open(table_file2) as t2:
                        for row_string2 in t2:
                            row_string = row_string1.strip()+','+row_string2.strip()
                            val_list = row_string.split(',')
                            val1 = int(val_list[col1_index])
                            val2 = int(val_list[col2_index])
                            omit_index = -1
                            if col1_index in select_indices and col2_index not in select_indices:
                                omit_index = col2_index
                            elif col1_index not in select_indices and col2_index in select_indices:
                                omit_index = col1_index
                            op = cond_ops[0]['operator']
                            if satisfy_operator(op, val1, val2):
                                mod_val_list = list()
                                for i in range(0,len(val_list)):
                                    if i == col2_index:
                                        continue
                                    if i in select_indices:
                                        mod_val_list.append(val_list[i])
                                print ",".join(mod_val_list)


    else:
        with open(table_file1) as t1:
            for row_string1 in t1:
                with open(table_file2) as t2:
                    for row_string2 in t2:
                        row_string = row_string1.strip()+','+row_string2.strip()
                        row_list = row_string.split(',')
                        mod_row_list = list()
                        for i in range(0,len(row_list)):
                            if i in select_indices:
                                mod_row_list.append(row_list[i])
                        print ",".join(mod_row_list)
        