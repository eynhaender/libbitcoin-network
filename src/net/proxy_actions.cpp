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
#include <bitcoin/network/net/proxy.hpp>

#include <utility>
#include <bitcoin/network/define.hpp>
#include <bitcoin/network/log/log.hpp>
#include <bitcoin/network/messages/messages.hpp>

namespace libbitcoin {
namespace network {

// Shared pointers required in handler parameters so closures control lifetime.
BC_PUSH_WARNING(NO_VALUE_OR_CONST_REF_SHARED_PTR)
BC_PUSH_WARNING(SMART_PTR_NOT_NEEDED)
BC_PUSH_WARNING(NO_THROW_IN_NOEXCEPT)

using namespace system;
using namespace std::placeholders;

// Wait (all).
// ----------------------------------------------------------------------------

void proxy::wait(result_handler&& handler) NOEXCEPT
{
    socket_->wait(std::move(handler));
}

void proxy::cancel(result_handler&& handler) NOEXCEPT
{
    socket_->cancel(std::move(handler));
}

//  WS (generic, framed).
// ----------------------------------------------------------------------------

// flat_buffer must have configured max_size, which will be allocated.
void proxy::read(http::flat_buffer& out, count_handler&& handler) NOEXCEPT
{
    do_reading();
    socket_->ws_read(out, std::move(handler));
}

void proxy::write(const asio::const_buffer& in, bool binary,
    count_handler&& handler) NOEXCEPT
{
    writer call = std::bind(&proxy::do_ws_write,
        shared_from_this(), in, binary, std::move(handler));

    boost::asio::dispatch(strand(),
        std::bind(&proxy::do_write,
            shared_from_this(), std::move(call)));
}

// private
void proxy::do_ws_write(const asio::const_buffer& payload, bool binary,
    const count_handler& handler) NOEXCEPT
{
    socket_->ws_write({ payload.data(), payload.size() }, binary,
        std::bind(&proxy::handle_write,
            shared_from_this(), _1, _2, handler));
}

//  TCP (generic, fixed size).
// ----------------------------------------------------------------------------

void proxy::read(const asio::mutable_buffer& out,
    count_handler&& handler) NOEXCEPT
{
    do_reading();
    socket_->tcp_read(out, std::move(handler));
}

void proxy::write(const asio::const_buffer& in,
    count_handler&& handler) NOEXCEPT
{
    writer call = std::bind(&proxy::do_tcp_write,
        shared_from_this(), in, std::move(handler));

    boost::asio::dispatch(strand(),
        std::bind(&proxy::do_write,
            shared_from_this(), std::move(call)));
}

// private
void proxy::do_tcp_write(const asio::const_buffer& payload,
    const count_handler& handler) NOEXCEPT
{
    socket_->tcp_write({ payload.data(), payload.size() },
        std::bind(&proxy::handle_write,
            shared_from_this(), _1, _2, handler));
}

// RPC (TCP: electrum/stratum_v1, WS: btcd).
// ----------------------------------------------------------------------------

// flat_buffer must have configured max_size, which will be allocated.
void proxy::read(http::flat_buffer& buffer, rpc::request& request,
    count_handler&& handler) NOEXCEPT
{
    do_reading();
    socket_->rpc_read(buffer, request, std::move(handler));
}

void proxy::write(rpc::response&& response, count_handler&& handler) NOEXCEPT
{
    // Pointer ships moveable message through the send queue.
    const auto out = move_shared(std::move(response));
    writer call = std::bind(&proxy::do_response_write,
        shared_from_this(), out, std::move(handler));

    boost::asio::dispatch(strand(),
        std::bind(&proxy::do_write,
            shared_from_this(), std::move(call)));
}

void proxy::write(rpc::request&& notification, count_handler&& handler) NOEXCEPT
{
    // Pointer ships moveable message through the send queue.
    const auto out = move_shared(std::move(notification));
    writer call = std::bind(&proxy::do_notification_write,
        shared_from_this(), out, std::move(handler));

    boost::asio::dispatch(strand(),
        std::bind(&proxy::do_write,
            shared_from_this(), std::move(call)));
}

// private
void proxy::do_response_write(const rpc::response_ptr& response,
    const count_handler& handler) NOEXCEPT
{
    socket_->rpc_write(std::move(*response),
        std::bind(&proxy::handle_write,
            shared_from_this(), _1, _2, handler));
}

// private
void proxy::do_notification_write(const rpc::request_ptr& notification,
    const count_handler& handler) NOEXCEPT
{
    socket_->rpc_notify(std::move(*notification),
        std::bind(&proxy::handle_write,
            shared_from_this(), _1, _2, handler));
}

// HTTP/WS (generic/rpc).
// ----------------------------------------------------------------------------

// flat_buffer must have configured max_size, which will be allocated.
void proxy::read(http::flat_buffer& buffer, http::request& request,
    count_handler&& handler) NOEXCEPT
{
    do_reading();
    socket_->http_read(buffer, request, std::move(handler));
}

void proxy::write(http::response&& response,
    count_handler&& handler) NOEXCEPT
{
    if (socket_->websocket())
    {
        // Pointer ships moveable message through the send queue.
        const auto out = move_shared(std::move(response));
        writer call = std::bind(&proxy::do_http_write,
            shared_from_this(), out, std::move(handler));

        boost::asio::dispatch(strand(),
            std::bind(&proxy::do_write,
                shared_from_this(), std::move(call)));
    }
    else
    {
        // http is half duplex so there is no interleave risk.
        socket_->http_write(std::move(response), std::move(handler));
    }
}

// private
void proxy::do_http_write(const http::response_ptr& response,
    const count_handler& handler) NOEXCEPT
{
    socket_->http_write(std::move(*response),
        std::bind(&proxy::handle_write,
            shared_from_this(), _1, _2, handler));
}

// SSE (HTTP streaming only).
// ----------------------------------------------------------------------------

void proxy::sse_start(const socket::sse_state::ptr& state,
    count_handler&& handler) NOEXCEPT
{
    do_writing();
    socket_->sse_start(state, std::move(handler));
}

void proxy::sse_write(const socket::sse_state::ptr& state,
    std::string&& event, count_handler&& handler) NOEXCEPT
{
    do_writing();
    socket_->sse_write(state, std::move(event), std::move(handler));
}

void proxy::sse_close(const socket::sse_state::ptr& state,
    count_handler&& handler) NOEXCEPT
{
    socket_->sse_close(state, std::move(handler));
}

BC_POP_WARNING()
BC_POP_WARNING()
BC_POP_WARNING()

} // namespace network
} // namespace libbitcoin
