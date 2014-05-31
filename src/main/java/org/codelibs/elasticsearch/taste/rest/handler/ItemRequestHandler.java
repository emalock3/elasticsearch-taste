package org.codelibs.elasticsearch.taste.rest.handler;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.rest.exception.OperationFailedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class ItemRequestHandler extends RequestHandler {
    public ItemRequestHandler(final Settings settings, final Client client) {
        super(settings, client);
    }

    public boolean hasItem(final Map<String, Object> requestMap) {
        return requestMap.containsKey("item");
    }

    @Override
    public void process(final RestRequest request, final RestChannel channel,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final Chain chain) {
        final String index = request
                .param("item_index", request.param("index"));
        final String itemType = request.param("item_type",
                TasteConstants.ITEM_TYPE);
        final String itemIdField = request.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        @SuppressWarnings("unchecked")
        final Map<String, Object> itemMap = (Map<String, Object>) requestMap
                .get("item");
        if (itemMap == null) {
            throw new InvalidParameterException("Item is null.");
        }
        final Object id = itemMap.get("id");
        if (id == null) {
            throw new InvalidParameterException("Item ID is null.");
        }

        try {
            client.prepareSearch(index).setTypes(itemType)
                    .setQuery(QueryBuilders.termQuery("id", id))
                    .addField(itemIdField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute(new ActionListener<SearchResponse>() {

                        @Override
                        public void onResponse(final SearchResponse response) {
                            try {
                                validateRespose(response);
                                final String updateType = request
                                        .param("update");

                                final SearchHits hits = response.getHits();
                                if (hits.getTotalHits() == 0) {
                                    handleItemCreation(request, channel,
                                            requestMap, paramMap, itemMap,
                                            index, itemType, itemIdField,
                                            timestampField, chain);
                                } else {
                                    final SearchHit[] searchHits = hits
                                            .getHits();
                                    final SearchHitField field = searchHits[0]
                                            .getFields().get(itemIdField);
                                    if (field != null) {
                                        final Number itemId = field.getValue();
                                        if (itemId != null) {
                                            if (TasteConstants.TRUE
                                                    .equalsIgnoreCase(updateType)
                                                    || TasteConstants.YES
                                                            .equalsIgnoreCase(updateType)) {
                                                handleItemUpdate(request,
                                                        channel, requestMap,
                                                        paramMap, itemMap,
                                                        index, itemType,
                                                        itemIdField,
                                                        timestampField,
                                                        itemId.longValue(),
                                                        OpType.INDEX, chain);
                                            } else {
                                                paramMap.put(itemIdField,
                                                        itemId.longValue());
                                                chain.process(request, channel,
                                                        requestMap, paramMap);
                                            }
                                            return;
                                        }
                                    }
                                    throw new OperationFailedException(
                                            "Item does not have " + itemIdField
                                                    + ": " + searchHits[0]);
                                }
                            } catch (final Exception e) {
                                onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            @SuppressWarnings("unchecked")
                            List<Throwable> errorList = (List<Throwable>) paramMap
                                    .get(ERROR_LIST);
                            if (errorList == null) {
                                errorList = new ArrayList<>();
                                paramMap.put(ERROR_LIST, errorList);
                            }
                            if (errorList.size() >= maxRetryCount) {
                                sendErrorResponse(request, channel, t);
                            } else {
                                errorList.add(t);
                                handleItemIndexCreation(request, channel,
                                        requestMap, paramMap, chain);
                            }
                        }
                    });
        } catch (final Exception e) {
            @SuppressWarnings("unchecked")
            List<Throwable> errorList = (List<Throwable>) paramMap
                    .get(ERROR_LIST);
            if (errorList == null) {
                errorList = new ArrayList<>();
                paramMap.put(ERROR_LIST, errorList);
            }
            if (errorList.size() >= maxRetryCount) {
                sendErrorResponse(request, channel, e);
            } else {
                errorList.add(e);
                handleItemIndexCreation(request, channel, requestMap, paramMap,
                        chain);
            }
        }
    }

    private void handleItemIndexCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final Chain chain) {
        final String index = request
                .param("item_index", request.param("index"));

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            handleItemMappingCreation(request, channel,
                                    requestMap, paramMap, chain);
                        } else {
                            client.admin()
                                    .indices()
                                    .prepareCreate(index)
                                    .execute(
                                            new ActionListener<CreateIndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        final CreateIndexResponse createIndexResponse) {
                                                    if (createIndexResponse
                                                            .isAcknowledged()) {
                                                        handleItemMappingCreation(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap, chain);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + index));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        final Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemMappingCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final Chain chain) {
        final String index = request
                .param("item_index", request.param("index"));
        final String type = request
                .param("item_type", TasteConstants.ITEM_TYPE);
        final String itemIdField = request.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(timestampField)//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // item_id
                    .startObject(itemIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // id
                    .startObject("id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();

            client.admin().indices().preparePutMapping(index).setType(type)
                    .setSource(builder)
                    .execute(new ActionListener<PutMappingResponse>() {

                        @Override
                        public void onResponse(
                                final PutMappingResponse queueMappingResponse) {
                            if (queueMappingResponse.isAcknowledged()) {
                                process(request, channel, requestMap, paramMap,
                                        chain);
                            } else {
                                onFailure(new OperationFailedException(
                                        "Failed to create mapping for " + index
                                                + "/" + type));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            sendErrorResponse(request, channel, t);
                        }
                    });
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }
    }

    private void handleItemCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField, final Chain chain) {
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(itemIdField)
                .addSort(itemIdField, SortOrder.DESC).setSize(1)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(final SearchResponse response) {
                        try {
                            validateRespose(response);

                            Number currentId = null;
                            final SearchHits hits = response.getHits();
                            if (hits.getTotalHits() != 0) {
                                final SearchHit[] searchHits = hits.getHits();
                                final SearchHitField field = searchHits[0]
                                        .getFields().get(itemIdField);
                                if (field != null) {
                                    currentId = field.getValue();
                                }
                            }
                            final Long itemId;
                            if (currentId == null) {
                                itemId = Long.valueOf(1);
                            } else {
                                itemId = Long.valueOf(currentId.longValue() + 1);
                            }
                            handleItemUpdate(request, channel, requestMap,
                                    paramMap, itemMap, index, type,
                                    itemIdField, timestampField, itemId,
                                    OpType.CREATE, chain);
                        } catch (final Exception e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemUpdate(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField, final Long itemId,
            final OpType opType, final Chain chain) {
        itemMap.put(itemIdField, itemId);
        itemMap.put(timestampField, new Date());
        client.prepareIndex(index, type, itemId.toString()).setSource(itemMap)
                .setRefresh(true).setOpType(opType)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        paramMap.put(itemIdField, itemId);
                        chain.process(request, channel, requestMap, paramMap);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

}