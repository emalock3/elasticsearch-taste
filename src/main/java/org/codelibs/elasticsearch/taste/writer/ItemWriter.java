package org.codelibs.elasticsearch.taste.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.recommender.RecommendedItem;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ItemWriter extends ObjectWriter {
    
    private static final ESLogger logger = Loggers.getLogger(ItemWriter.class);

    protected String targetIdField;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected String valueField = TasteConstants.VALUE_FIELD;

    protected String itemsField = TasteConstants.ITEMS_FILED;

    protected boolean verbose = false;

    protected String targetIndex;

    protected String targetType;

    protected String itemIndex;

    protected String itemType;

    protected Cache<Long, Map<String, Object>> cache;
    
    private final BlockingQueue<Map<String, Object>> rootObjQueue;

    public ItemWriter(final Client client, final String index,
            final String type, final String targetIdField) {
        super(client, index, type);
        this.targetIdField = targetIdField;
        this.rootObjQueue = new ArrayBlockingQueue<>(1000);
    }

    public void write(final long id,
            final List<RecommendedItem> recommendedItems) {
        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(targetIdField, id);
        if (verbose) {
            final GetResponse response = client
                    .prepareGet(targetIndex, targetType, Long.toString(id))
                    .execute().actionGet();
            if (response.isExists()) {
                final Map<String, Object> map = response.getSourceAsMap();
                map.remove(targetIdField);
                rootObj.putAll(map);
            }
        }
        final List<Map<String, Object>> itemList = new ArrayList<>();
        for (final RecommendedItem recommendedItem : recommendedItems) {
            final Map<String, Object> item = new HashMap<>();
            item.put(itemIdField, recommendedItem.getItemID());
            item.put(valueField, recommendedItem.getValue());
            if (verbose) {
                final Map<String, Object> map = getItemMap(recommendedItem
                        .getItemID());
                if (map != null) {
                    item.putAll(map);
                }
            }
            itemList.add(item);
        }
        rootObj.put(itemsField, itemList);
        
        if (!rootObjQueue.offer(rootObj)) {
            synchronized(rootObjQueue) {
                writeBulkObjs();
                rootObjQueue.add(rootObj);
            }
        }

    }
    
    private void writeBulkObjs() {
        synchronized(rootObjQueue) {
            if (rootObjQueue.size() > 0) {
                BulkRequestBuilder brb = client.prepareBulk();
                for (Map<String, Object> ro : rootObjQueue) {
                    brb.add(client.prepareIndex(index, type).setSource(ro));
                }
                brb.execute(new ActionListener<BulkResponse>() {

                    @Override
                    public void onResponse(final BulkResponse bulkResponse) {
                        if (logger.isDebugEnabled()) {
                            for (BulkItemResponse response : bulkResponse.getItems()) {
                                logger.debug("Response: {}/{}/{}, Version: {}",
                                        response.getIndex(), response.getType(),
                                        response.getId(), response.getVersion());
                            }
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to bulk write ", e);
                    }
                });
                rootObjQueue.clear();
            }
        }
    }

    @Override
    public void close() throws IOException {
        writeBulkObjs();
        super.close();
    }

    protected Map<String, Object> getItemMap(final long itemID) {
        if (cache != null) {
            final Map<String, Object> map = cache.getIfPresent(itemID);
            if (map != null) {
                return map;
            }
        }
        final GetResponse response = client
                .prepareGet(itemIndex, itemType, Long.toString(itemID))
                .execute().actionGet();
        if (response.isExists()) {
            final Map<String, Object> map = response.getSourceAsMap();
            map.remove(itemIdField);
            map.remove(valueField);
            cache.put(itemID, map);
            return map;
        }
        return null;
    }

    public void setTargetIndex(final String targetIndex) {
        this.targetIndex = targetIndex;
    }

    public void setTargetType(final String targetType) {
        this.targetType = targetType;
    }

    public void setItemIdField(final String itemIdField) {
        this.itemIdField = itemIdField;
    }

    public void setValueField(final String valueField) {
        this.valueField = valueField;
    }

    public void setItemsField(final String itemsField) {
        this.itemsField = itemsField;
    }

    public void setItemIndex(final String itemIndex) {
        this.itemIndex = itemIndex;
    }

    public void setItemType(final String itemType) {
        this.itemType = itemType;
    }

    public void setCache(final Cache<Long, Map<String, Object>> cache) {
        this.cache = cache;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

}
