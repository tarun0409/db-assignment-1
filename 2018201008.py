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
                    query_dict['tables'].append(tab)
        elif tokens[i]=='where':
            condition = list()
            for j in range((i+1),len(tokens)):
                if tokens[j]==';':
                    break
                condition.append(tokens[j])
            query_dict['condition'] = parse_condition(condition)
    return query_dict

def parse_metadata(file_path):
    # table_list = list()
    table = dict()
    line_one = False
    curr_table_name = None
    with open(file_path) as f:
        for line in f:
            token = line.strip()
            if token == '<begin_table>':
                # table = dict()
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

query_str = sys.argv[1]
query_obj = parse_query(query_str)
metadata = parse_metadata('metadata.txt')
qt = query_obj['tables']
if len(qt)==1:
    table_name = qt[0]
    print ",".join(metadata[table_name])
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
            if condition is None:
                print row_string
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
                if satisfied_conditions != total_conditions:
                    row_approved = False
                if row_approved:
                    print row_string

