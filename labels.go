package ruler

func mergeLabelSets(logger Logger, groupName, ruleName string, base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}

	out := make(map[string]string, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}

	for k, v := range overlay {
		if prev, ok := out[k]; ok && prev != v {
			logger.Debugf("label %q=%q for rule %q.%q overwritten with %q", k, prev, groupName, ruleName, v)
		}
		out[k] = v
	}
	
	return out
}
