/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.indexer.elasticsearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GlobalTimeoutClient extends FilterClient {
    private final long timeout;
    private final TimeUnit unit;

    public GlobalTimeoutClient(Client in, long timeout, TimeUnit unit) {
        super(in);

        checkArgument(timeout > 0);
        this.timeout = timeout;
        this.unit = checkNotNull(unit);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        super.doExecute(new GlobalTimeoutAction<>(action, timeout, unit), request, listener);
    }

    public static class GlobalTimeoutAction<Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            extends Action<Request, Response, RequestBuilder> {
        private final Action<Request, Response, RequestBuilder> action;
        private final TimeValue timeout;

        public GlobalTimeoutAction(final Action<Request, Response, RequestBuilder> action, long duration, TimeUnit timeUnit) {
            super(action.name());
            this.action = checkNotNull(action);
            this.timeout = new TimeValue(duration, timeUnit);
        }

        @Override
        public TransportRequestOptions transportOptions(Settings settings) {
            final TransportRequestOptions result = super.transportOptions(settings);
            return result.timeout() == null ? result.withTimeout(timeout) : result;
        }

        @Override
        public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return action.newRequestBuilder(client);
        }

        @Override
        public Response newResponse() {
            return action.newResponse();
        }
    }
}
