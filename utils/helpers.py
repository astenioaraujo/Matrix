def conta_para_ordenacao(valor):
    try:
        return int(float(str(valor).strip()))
    except (TypeError, ValueError):
        return 999999

def cor(v, minv, maxv):
    v = float(v or 0)
    minv = float(minv or 0)
    maxv = float(maxv or 0)

    if maxv == minv:
        return ""

    pos = (v - minv) / (maxv - minv)

    if pos < 0.25:
        return "background:#f8696b"
    elif pos < 0.50:
        return "background:#f4a582"
    elif pos < 0.75:
        return "background:#92d050"
    else:
        return "background:#00b050"