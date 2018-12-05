/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.routing.selector;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.lineage.SegmentGroup;
import com.linkedin.pinot.common.lineage.SegmentMergeLineage;
import com.linkedin.pinot.common.lineage.SegmentMergeLineageAccessHelper;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Segment selector for merged segments
 */
public class MergedSegmentSelector implements SegmentSelector {
  private String _tableNameWithType;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private volatile SegmentGroup _rootSegmentGroup;

  @Override
  public void init(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
  }

  @Override
  public void computeOnExternalViewChange() {
    SegmentMergeLineage segmentMergeLineage = SegmentMergeLineageAccessHelper.getSegmentMergeLineage(_propertyStore,
        _tableNameWithType);
    _rootSegmentGroup = segmentMergeLineage.getMergeLineageRootSegmentGroup();
  }

  @Override
  public Set<String> selectSegments(RoutingTableLookupRequest request, Set<String> segmentsToQuery) {
    Set<String> selectedSegments = new HashSet<>(segmentsToQuery);
    for (SegmentGroup segmentGroup : _rootSegmentGroup.getChildrenGroups()) {
      computeSelectionProcessForSegmentGroup(segmentGroup, selectedSegments, segmentsToQuery);
    }
    return selectedSegments;
  }

  private void computeSelectionProcessForSegmentGroup(SegmentGroup segmentGroup, Set<String> selectedSegments,
      Set<String> availableSegments) {
    Set<String> segmentsForGroup = segmentGroup.getSegments();

    if (availableSegments.containsAll(segmentsForGroup)) {
      // If we pick the current group node, we delete all segments covered by children groups
      if (segmentGroup.getChildrenGroups() == null || segmentGroup.getChildrenGroups().isEmpty()) {
        return;
      }
      for (SegmentGroup child: segmentGroup.getChildrenGroups()) {
        removeSegmentsForSegmentGroup(child, selectedSegments);
      }
    } else {
      // If the current group is not picked, we compute the selection recursively for children nodes
      selectedSegments.removeAll(segmentsForGroup);
      for (SegmentGroup child: segmentGroup.getChildrenGroups()) {
        computeSelectionProcessForSegmentGroup(child, selectedSegments, availableSegments);
      }
    }
  }

  private void removeSegmentsForSegmentGroup(SegmentGroup segmentGroup, Set<String> selectedSegments) {
    Set<String> segmentsForGroup = segmentGroup.getSegments();
    selectedSegments.removeAll(segmentsForGroup);

    if (segmentGroup.getChildrenGroups() == null || segmentGroup.getChildrenGroups().isEmpty()) {
      return;
    }

    for (SegmentGroup child : segmentGroup.getChildrenGroups()) {
      removeSegmentsForSegmentGroup(child, selectedSegments);
    }
  }
}
