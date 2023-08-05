from typing import Any, Optional


def IDX_INT_ERROR_MSG(q: str, query: 'list[str]', prev_q: str) -> str:
    prev_q = prev_q or ""
    q = str(q)
    query = '.'.join([prev_q] + [q] + query)
    n: int = 75 + len(prev_q) + len(q)
    msg: str = f"expecting integer / empty string but '{q}' is given in `query`: '{query}'.\n{' ' * n}^"
    return msg

def IDX_RANGE_ERROR_MSG(q: str, query: 'list[str]', prev_q: str) -> str:
    prev_q = prev_q or ""
    q = str(q)
    query = '.'.join([prev_q] + [q] + query)
    n: int = 50 + len(prev_q)
    msg: str = f"list index out of range in `query`: '{query}'.\n{' ' * n}^"
    return msg

def pjq(
    json_dict: 'list | dict', 
    query: 'list[str] | str', 
    default: Optional[Any] = None, 
    sep: str = ".", 
    idx_sep: str = ",", 
    trim: bool = True, 
    prev_q: Optional[str] = None
) -> 'list | dict | None':
    query = query.split(sep) if isinstance(query, str) else query
    # Cannot pop query index otherwise affecting the for loop in list
    q: str = query[0]
    query: list[str] = query[1:]
    if json_dict == default:
        # If `default` is set to a list of dict, it cannot go through this
        return default
    
    elif isinstance(json_dict, dict):
        json_dict = json_dict.get(q, default)
        
        if query:
            return pjq(json_dict, query, default, sep, idx_sep, trim=trim, prev_q=q)
        return json_dict
    
    elif isinstance(json_dict, list):
        if q:
            try:
                idx: list[int] = [int(i) for i in q.split(idx_sep)]
                json_dict = [json_dict[i] for i in idx]
            except ValueError:
                msg: str = IDX_INT_ERROR_MSG(q, query, prev_q)
                raise ValueError(msg)
            except IndexError:
                msg: str = IDX_RANGE_ERROR_MSG(q, query, prev_q)
                raise IndexError(msg)
        if query:
            json_dict = [pjq(jd, query, default, sep, idx_sep, trim=trim, prev_q=q) for jd in json_dict]
        
        if trim:
            json_dict = json_dict[0] if len(json_dict) == 1 else json_dict
        
        return json_dict
    
    else:
        return None 