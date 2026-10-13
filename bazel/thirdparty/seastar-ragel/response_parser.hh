/*
* This file is open source software, licensed to you under the terms
* of the Apache License, Version 2.0 (the "License").  See the NOTICE file
* distributed with this work for additional information regarding copyright
* ownership.  You may not use this file except in compliance with the License.
*
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
/*
* Copyright (C) 2015 Cloudius Systems, Ltd.
*/

#pragma once

#include <memory>
#include <unordered_map>

#include <seastar/core/ragel.hh>
#include <seastar/http/reply.hh>

namespace seastar {
	
	
	
	
	
	
	
	
	class http_response_parser : public ragel_parser_base<http_response_parser> {
		
		static const int start = 1;
		static const int error = 0;
		
		static const int en_main = 1;
		
		
		
		public:
		enum class state {
			error,
			eof,
			done,
		};
		std::unique_ptr<http::reply> _rsp;
		sstring _field_name;
		sstring _value;
		state _state;
		sstring _error_message;
		
		public:
		void init() {
			init_base();
			_rsp.reset(new http::reply());
			_state = state::eof;
			_error_message = {};
			
			{
				_fsm_cs = (int)start;
			}
			
			
		}
		char* parse(char* p, char* pe, char* eof) {
			// Save the start pointer to compute offsets for error context.
			char* start_ptr = p;
			sstring_builder::guard g(_builder, p, pe);
			auto str = [this, &g, &p] { g.mark_end(p); return get_str(); };
			bool done = false;
			if (p != pe) {
				_state = state::error;
			}
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmisleading-indentation"
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
			
			{
				switch ( _fsm_cs ) {
					case 1:
					goto st_case_1;
					case 0:
					goto st_case_0;
					case 2:
					goto st_case_2;
					case 3:
					goto st_case_3;
					case 4:
					goto st_case_4;
					case 5:
					goto st_case_5;
					case 6:
					goto st_case_6;
					case 7:
					goto st_case_7;
					case 8:
					goto st_case_8;
					case 9:
					goto st_case_9;
					case 10:
					goto st_case_10;
					case 11:
					goto st_case_11;
					case 12:
					goto st_case_12;
					case 13:
					goto st_case_13;
					case 14:
					goto st_case_14;
					case 15:
					goto st_case_15;
					case 16:
					goto st_case_16;
					case 17:
					goto st_case_17;
					case 27:
					goto st_case_27;
					case 18:
					goto st_case_18;
					case 19:
					goto st_case_19;
					case 20:
					goto st_case_20;
					case 21:
					goto st_case_21;
					case 22:
					goto st_case_22;
					case 23:
					goto st_case_23;
					case 24:
					goto st_case_24;
					case 25:
					goto st_case_25;
					case 26:
					goto st_case_26;
				}
				p+= 1;
				st_case_1:
				if ( p == pe )
					goto _out1;
				if ( ( (*( p))) == 72 ) {
					goto _st2;
				}
				goto _st0;
				_st0:
				st_case_0:
				goto _out0;
				_st2:
				p+= 1;
				st_case_2:
				if ( p == pe )
					goto _out2;
				if ( ( (*( p))) == 84 ) {
					goto _st3;
				}
				goto _st0;
				_st3:
				p+= 1;
				st_case_3:
				if ( p == pe )
					goto _out3;
				if ( ( (*( p))) == 84 ) {
					goto _st4;
				}
				goto _st0;
				_st4:
				p+= 1;
				st_case_4:
				if ( p == pe )
					goto _out4;
				if ( ( (*( p))) == 80 ) {
					goto _st5;
				}
				goto _st0;
				_st5:
				p+= 1;
				st_case_5:
				if ( p == pe )
					goto _out5;
				if ( ( (*( p))) == 47 ) {
					goto _st6;
				}
				goto _st0;
				_st6:
				p+= 1;
				st_case_6:
				if ( p == pe )
					goto _out6;
				if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
					goto _ctr7;
				}
				goto _st0;
				_ctr7:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st7;
				_st7:
				p+= 1;
				st_case_7:
				if ( p == pe )
					goto _out7;
				if ( ( (*( p))) == 46 ) {
					goto _st8;
				}
				goto _st0;
				_st8:
				p+= 1;
				st_case_8:
				if ( p == pe )
					goto _out8;
				if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
					goto _st9;
				}
				goto _st0;
				_st9:
				p+= 1;
				st_case_9:
				if ( p == pe )
					goto _out9;
				if ( ( (*( p))) == 32 ) {
					goto _ctr11;
				}
				if ( 9 <= ( (*( p))) && ( (*( p))) <= 13 ) {
					goto _ctr11;
				}
				goto _st0;
				_ctr11:
				{
					
					_rsp->_version = str();
				}
				
				
				goto _st10;
				_st10:
				p+= 1;
				st_case_10:
				if ( p == pe )
					goto _out10;
				if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
					goto _ctr13;
				}
				goto _st0;
				_ctr13:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st11;
				_st11:
				p+= 1;
				st_case_11:
				if ( p == pe )
					goto _out11;
				if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
					goto _st12;
				}
				goto _st0;
				_st12:
				p+= 1;
				st_case_12:
				if ( p == pe )
					goto _out12;
				if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
					goto _st13;
				}
				goto _st0;
				_st13:
				p+= 1;
				st_case_13:
				if ( p == pe )
					goto _out13;
				if ( ( (*( p))) == 32 ) {
					goto _ctr17;
				}
				if ( 9 <= ( (*( p))) && ( (*( p))) <= 13 ) {
					goto _ctr17;
				}
				goto _st0;
				_ctr17:
				{
					
					_rsp->_status = static_cast<http::reply::status_type>(std::atoi(str().c_str()));
				}
				
				
				goto _st14;
				_st14:
				p+= 1;
				st_case_14:
				if ( p == pe )
					goto _out14;
				switch( ( (*( p))) ) {
					case 10: {
						goto _st0;
					}
					case 13: {
						goto _st15;
					}
				}
				goto _st14;
				_st15:
				p+= 1;
				st_case_15:
				if ( p == pe )
					goto _out15;
				if ( ( (*( p))) == 10 ) {
					goto _st16;
				}
				goto _st0;
				_st16:
				p+= 1;
				st_case_16:
				if ( p == pe )
					goto _out16;
				switch( ( (*( p))) ) {
					case 13: {
						goto _st17;
					}
					case 33: {
						goto _ctr22;
					}
					case 124: {
						goto _ctr22;
					}
					case 126: {
						goto _ctr22;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr22;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr22;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr22;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr22;
						}
					} else {
						goto _ctr22;
					}
				} else {
					goto _ctr22;
				}
				goto _st0;
				_ctr34:
				{
					
					auto [iter, inserted] = _rsp->_headers.try_emplace(_field_name, std::move(_value));
					if (!inserted) {
						// RFC 7230, section 3.2.2.  Field Parsing:
						// A recipient MAY combine multiple header fields with the same field name into one
						// "field-name: field-value" pair, without changing the semantics of the message,
						// by appending each subsequent field value to the combined field value in order, separated by a comma.
						iter->second += sstring(",") + std::move(_value);
					}
				}
				
				
				goto _st17;
				_ctr44:
				{
					
					// RFC 7230, section 3.2.4.  Field Order:
					// A server that receives an obs-fold in a request message that is not
					// within a message/http container MUST either reject the message [...]
					// or replace each received obs-fold with one or more SP octets [...]
					_rsp->_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				goto _st17;
				_st17:
				p+= 1;
				st_case_17:
				if ( p == pe )
					goto _out17;
				if ( ( (*( p))) == 10 ) {
					goto _ctr23;
				}
				goto _st0;
				_ctr23:
				{
					
					done = true;
					{p+= 1; _fsm_cs = 27; goto _out;}
				}
				
				
				goto _st27;
				_st27:
				p+= 1;
				st_case_27:
				if ( p == pe )
					goto _out27;
				goto _st0;
				_ctr22:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st18;
				_ctr35:
				{
					
					auto [iter, inserted] = _rsp->_headers.try_emplace(_field_name, std::move(_value));
					if (!inserted) {
						// RFC 7230, section 3.2.2.  Field Parsing:
						// A recipient MAY combine multiple header fields with the same field name into one
						// "field-name: field-value" pair, without changing the semantics of the message,
						// by appending each subsequent field value to the combined field value in order, separated by a comma.
						iter->second += sstring(",") + std::move(_value);
					}
				}
				
				
				{
					
					g.mark_start(p);
				}
				
				
				goto _st18;
				_ctr45:
				{
					
					// RFC 7230, section 3.2.4.  Field Order:
					// A server that receives an obs-fold in a request message that is not
					// within a message/http container MUST either reject the message [...]
					// or replace each received obs-fold with one or more SP octets [...]
					_rsp->_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				{
					
					g.mark_start(p);
				}
				
				
				goto _st18;
				_st18:
				p+= 1;
				st_case_18:
				if ( p == pe )
					goto _out18;
				switch( ( (*( p))) ) {
					case 33: {
						goto _st18;
					}
					case 58: {
						goto _ctr25;
					}
					case 124: {
						goto _st18;
					}
					case 126: {
						goto _st18;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _st18;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _st18;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _st18;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _st18;
						}
					} else {
						goto _st18;
					}
				} else {
					goto _st18;
				}
				goto _st0;
				_ctr25:
				{
					
					_field_name = str();
				}
				
				
				goto _st19;
				_st19:
				p+= 1;
				st_case_19:
				if ( p == pe )
					goto _out19;
				switch( ( (*( p))) ) {
					case 9: {
						goto _st19;
					}
					case 13: {
						goto _ctr28;
					}
					case 32: {
						goto _st19;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr27;
				_ctr27:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st20;
				_st20:
				p+= 1;
				st_case_20:
				if ( p == pe )
					goto _out20;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr30;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( ( (*( p))) > 8 ) {
					if ( 10 <= ( (*( p))) && ( (*( p))) <= 31 ) {
						goto _st0;
					}
				} else if ( ( (*( p))) >= 0 ) {
					goto _st0;
				}
				goto _st20;
				_ctr28:
				{
					
					g.mark_start(p);
				}
				
				
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st21;
				_ctr30:
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st21;
				_st21:
				p+= 1;
				st_case_21:
				if ( p == pe )
					goto _out21;
				if ( ( (*( p))) == 10 ) {
					goto _st22;
				}
				goto _st0;
				_st22:
				p+= 1;
				st_case_22:
				if ( p == pe )
					goto _out22;
				switch( ( (*( p))) ) {
					case 9: {
						goto _ctr33;
					}
					case 13: {
						goto _ctr34;
					}
					case 32: {
						goto _ctr33;
					}
					case 33: {
						goto _ctr35;
					}
					case 124: {
						goto _ctr35;
					}
					case 126: {
						goto _ctr35;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr35;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr35;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr35;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr35;
						}
					} else {
						goto _ctr35;
					}
				} else {
					goto _ctr35;
				}
				goto _st0;
				_ctr33:
				{
					
					auto [iter, inserted] = _rsp->_headers.try_emplace(_field_name, std::move(_value));
					if (!inserted) {
						// RFC 7230, section 3.2.2.  Field Parsing:
						// A recipient MAY combine multiple header fields with the same field name into one
						// "field-name: field-value" pair, without changing the semantics of the message,
						// by appending each subsequent field value to the combined field value in order, separated by a comma.
						iter->second += sstring(",") + std::move(_value);
					}
				}
				
				
				goto _st23;
				_ctr43:
				{
					
					// RFC 7230, section 3.2.4.  Field Order:
					// A server that receives an obs-fold in a request message that is not
					// within a message/http container MUST either reject the message [...]
					// or replace each received obs-fold with one or more SP octets [...]
					_rsp->_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				goto _st23;
				_st23:
				p+= 1;
				st_case_23:
				if ( p == pe )
					goto _out23;
				switch( ( (*( p))) ) {
					case 9: {
						goto _st23;
					}
					case 13: {
						goto _ctr38;
					}
					case 32: {
						goto _st23;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr37;
				_ctr37:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st24;
				_st24:
				p+= 1;
				st_case_24:
				if ( p == pe )
					goto _out24;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr40;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( ( (*( p))) > 8 ) {
					if ( 10 <= ( (*( p))) && ( (*( p))) <= 31 ) {
						goto _st0;
					}
				} else if ( ( (*( p))) >= 0 ) {
					goto _st0;
				}
				goto _st24;
				_ctr38:
				{
					
					g.mark_start(p);
				}
				
				
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st25;
				_ctr40:
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st25;
				_st25:
				p+= 1;
				st_case_25:
				if ( p == pe )
					goto _out25;
				if ( ( (*( p))) == 10 ) {
					goto _st26;
				}
				goto _st0;
				_st26:
				p+= 1;
				st_case_26:
				if ( p == pe )
					goto _out26;
				switch( ( (*( p))) ) {
					case 9: {
						goto _ctr43;
					}
					case 13: {
						goto _ctr44;
					}
					case 32: {
						goto _ctr43;
					}
					case 33: {
						goto _ctr45;
					}
					case 124: {
						goto _ctr45;
					}
					case 126: {
						goto _ctr45;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr45;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr45;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr45;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr45;
						}
					} else {
						goto _ctr45;
					}
				} else {
					goto _ctr45;
				}
				goto _st0;
				_out1: _fsm_cs = 1; goto _out; 
				_out0: _fsm_cs = 0; goto _out; 
				_out2: _fsm_cs = 2; goto _out; 
				_out3: _fsm_cs = 3; goto _out; 
				_out4: _fsm_cs = 4; goto _out; 
				_out5: _fsm_cs = 5; goto _out; 
				_out6: _fsm_cs = 6; goto _out; 
				_out7: _fsm_cs = 7; goto _out; 
				_out8: _fsm_cs = 8; goto _out; 
				_out9: _fsm_cs = 9; goto _out; 
				_out10: _fsm_cs = 10; goto _out; 
				_out11: _fsm_cs = 11; goto _out; 
				_out12: _fsm_cs = 12; goto _out; 
				_out13: _fsm_cs = 13; goto _out; 
				_out14: _fsm_cs = 14; goto _out; 
				_out15: _fsm_cs = 15; goto _out; 
				_out16: _fsm_cs = 16; goto _out; 
				_out17: _fsm_cs = 17; goto _out; 
				_out27: _fsm_cs = 27; goto _out; 
				_out18: _fsm_cs = 18; goto _out; 
				_out19: _fsm_cs = 19; goto _out; 
				_out20: _fsm_cs = 20; goto _out; 
				_out21: _fsm_cs = 21; goto _out; 
				_out22: _fsm_cs = 22; goto _out; 
				_out23: _fsm_cs = 23; goto _out; 
				_out24: _fsm_cs = 24; goto _out; 
				_out25: _fsm_cs = 25; goto _out; 
				_out26: _fsm_cs = 26; goto _out; 
				_out: {}
			}
			
			
#pragma GCC diagnostic pop
#ifdef __clang__
#pragma clang diagnostic pop
#endif
			if (done) {
				_state = state::done;
			} else if (p == eof) {
				_state = state::eof;
				_error_message = "Incomplete HTTP response header: reached end-of-file before parsing completed.";
			} else if (p != pe) {
				_state = state::error;
				// Get the error offset and extract a snippet from the current pointer.
				size_t offset = static_cast<size_t>(p - start_ptr);
				size_t available = static_cast<size_t>(pe - p);
				size_t context_len = std::min(available, 32ul);
				sstring encountered(p, context_len);
				_error_message = sstring("Parsing error at offset ") + to_sstring(offset) + ": encountered \"" + encountered +
				"\". Expected valid HTTP response header format (e.g., a complete start line with HTTP version, three-digit status code, header "
				"fields, and a terminating CRLF).";
			} else {
				p = nullptr;
			}
			return p;
		}
		auto get_parsed_response() {
			return std::move(_rsp);
		}
		bool eof() const {
			return _state == state::eof;
		}
		bool failed() const {
			return _state == state::error;
		}
		const sstring& error_message() const {
			return _error_message;
		}
	};
}
