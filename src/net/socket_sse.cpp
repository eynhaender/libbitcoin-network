/**
 * Copyright (c) 2011-2026 libbitcoin developers (see AUTHORS)
 *
 * This file is part of libbitcoin.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <bitcoin/network/net/socket.hpp>

#include <utility>
#include <bitcoin/network/define.hpp>
#include <bitcoin/network/log/log.hpp>

namespace libbitcoin {
namespace network {

using namespace std::placeholders;

BC_PUSH_WARNING(NO_THROW_IN_NOEXCEPT)

// sse_state
// ----------------------------------------------------------------------------

socket::sse_state::sse_state(unsigned version) NOEXCEPT
  : response(boost::beast::http::status::ok, version),
    serializer(response)
{
    namespace http = boost::beast::http;
    response.set(http::field::content_type,  "text/event-stream");
    response.set(http::field::cache_control, "no-cache");
    response.set(http::field::connection,    "keep-alive");
    response.chunked(true);
    response.body().data = nullptr;
    response.body().size = 0;
    response.body().more = true;
}

// SSE write methods
// ----------------------------------------------------------------------------

void socket::sse_start(const sse_state::ptr& state,
    count_handler&& handler) NOEXCEPT
{
    BC_ASSERT(stranded());
    BC_ASSERT(!websocket());
    try
    {
        VARIANT_DISPATCH_FUNCTION(
            boost::beast::http::async_write_header, get_tcp(),
            state->serializer,
            std::bind(&socket::handle_async, shared_from_this(),
                _1, _2, handler, "sse-start"));
    }
    catch (const std::exception& e)
    {
        LOGF("Exception @ sse_start: " << e.what());
        handler(error::operation_failed, {});
    }
}

void socket::sse_write(const sse_state::ptr& state, std::string&& event,
    count_handler&& handler) NOEXCEPT
{
    BC_ASSERT(stranded());
    BC_ASSERT(!websocket());
    try
    {
        state->pending = std::move(event);
        state->response.body().data = state->pending.data();
        state->response.body().size = state->pending.size();
        state->response.body().more = true;
        VARIANT_DISPATCH_FUNCTION(
            boost::beast::http::async_write, get_tcp(),
            state->serializer,
            [self = shared_from_this(), h = std::move(handler)](
                const boost_code& ec, size_t bytes) mutable NOEXCEPT
            {
                // buffer_body::writer returns need_buffer after each SSE chunk
                // to signal "chunk written, ready for more data" — treat as success.
                const auto mapped =
                    (ec == error::to_http_code(error::http_error_t::need_buffer))
                    ? boost_code{} : ec;
                self->handle_async(mapped, bytes, h, "sse-write");
            });
    }
    catch (const std::exception& e)
    {
        LOGF("Exception @ sse_write: " << e.what());
        handler(error::operation_failed, {});
    }
}

void socket::sse_close(const sse_state::ptr& state,
    count_handler&& handler) NOEXCEPT
{
    BC_ASSERT(stranded());
    BC_ASSERT(!websocket());
    try
    {
        state->response.body().data = nullptr;
        state->response.body().size = 0;
        state->response.body().more = false;
        VARIANT_DISPATCH_FUNCTION(
            boost::beast::http::async_write, get_tcp(),
            state->serializer,
            std::bind(&socket::handle_async, shared_from_this(),
                _1, _2, handler, "sse-close"));
    }
    catch (const std::exception& e)
    {
        LOGF("Exception @ sse_close: " << e.what());
        handler(error::operation_failed, {});
    }
}

BC_POP_WARNING()

} // namespace network
} // namespace libbitcoin
