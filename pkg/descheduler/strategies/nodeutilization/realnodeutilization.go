package nodeutilization

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

func validateRealNodeUtilizationParams(params *api.StrategyParameters) error {
	if params == nil || params.NodeRealUtilizationThresholds == nil {
		return fmt.Errorf("NodeRealUtilizationThresholds not set")
	}

	// At most one of include/exclude can be set
	if params.Namespaces != nil && len(params.Namespaces.Include) > 0 && len(params.Namespaces.Exclude) > 0 {
		return fmt.Errorf("only one of Include/Exclude namespaces can be set")
	}
	if params.ThresholdPriority != nil && params.ThresholdPriorityClassName != "" {
		return fmt.Errorf("only one of thresholdPriority and thresholdPriorityClassName can be set")
	}

	return nil
}

func RealNodeUtilization(ctx context.Context, client clientset.Interface, strategy api.DeschedulerStrategy, nodes []*v1.Node, podEvictor *evictions.PodEvictor) {
	if err := validateRealNodeUtilizationParams(strategy.Params); err != nil {
		klog.ErrorS(err, "Invalid RealNodeUtilization parameters")
		return
	}

	if strategy.Params.NodeRealUtilizationThresholds.WatcherAddress == "" {
		err := errors.New("WatcherAddress is null")
		klog.ErrorS( err, "Invalid RealNodeUtilization parameters")
		return
	}

	memoryTarget := strategy.Params.NodeRealUtilizationThresholds.TargetMemoryRate

	var includedNamespaces, excludedNamespaces []string
	if strategy.Params.Namespaces != nil {
		includedNamespaces = strategy.Params.Namespaces.Include
		excludedNamespaces = strategy.Params.Namespaces.Exclude
	}

	nodeFit := false
	if strategy.Params != nil {
		nodeFit = strategy.Params.NodeFit
	}

	thresholdPriority, err := utils.GetPriorityFromStrategyParams(ctx, client, strategy.Params)
	if err != nil {
		klog.ErrorS(err, "Failed to get threshold priority from strategy's params")
		return
	}

	evictable := podEvictor.Evictable(evictions.WithPriorityThreshold(thresholdPriority), evictions.WithNodeFit(nodeFit))

	mc, err := NewServiceClient(strategy.Params.NodeRealUtilizationThresholds.WatcherAddress)
	if err != nil {
		klog.ErrorS(err, "Failed to create Metrics Client")
		return
	}

	metrics, err := mc.GetWatcherMetrics()
	if err != nil {
		klog.ErrorS(err, "Failed to get Metrics from watcher")
		return
	}

	for _, node := range nodes {
		pods, err := podutil.ListPodsOnANode(
			ctx,
			client,
			node,
			podutil.WithFilter(evictable.IsEvictable),
			podutil.WithNamespaces(includedNamespaces),
			podutil.WithoutNamespaces(excludedNamespaces),
		)
		if err != nil {
			//no pods evicted as error encountered retrieving evictable Pods
			return
		}
		nodeMetrics := metrics.Data.NodeMetricsMap[node.Name]
		for _, m := range nodeMetrics.Metrics {
			if m.Type == "Memory" && m.Operator == "Latest" && m.Value > memoryTarget {
				if len(pods) == 0 {
					klog.Infof("no pods can be evicted on node:%s, namespace:%v", node.Name, includedNamespaces)
					break
				}
				if _, err := podEvictor.EvictPod(ctx, pods[1], node, "RealNodeUtilization"); err != nil {
					klog.ErrorS(err, "Error evicting pod")
					break
				}
			}
		}
	}
}
