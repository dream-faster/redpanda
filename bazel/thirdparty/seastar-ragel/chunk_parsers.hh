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
* Copyright (C) 2020 Cloudius Systems, Ltd.
*/

#pragma once

#include <seastar/core/ragel.hh>
#include <seastar/core/sstring.hh>
#include <unordered_map>

namespace seastar {
	
	
	
	
	
	
	
	class http_chunk_size_and_ext_parser : public ragel_parser_base<http_chunk_size_and_ext_parser> {
		
		static const int start = 1;
		static const int error = 0;
		
		static const int en_main = 1;
		
		
		
		public:
		enum class state {
			error,
			eof,
			done,
		};
		std::unordered_map<sstring, sstring> _extensions;
		sstring _name;
		sstring _value;
		sstring _size;
		state _state;
		public:
		void init() {
			init_base();
			_extensions = std::unordered_map<sstring, sstring>();
			_value = "";
			_size = "";
			_state = state::eof;
			
			{
				_fsm_cs = (int)start;
			}
			
			
		}
		char* parse(char* p, char* pe, char* eof) {
			sstring_builder::guard g(_builder, p, pe);
			[[maybe_unused]] auto str = [this, &g, &p] { g.mark_end(p); return get_str(); };
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
					case 12:
					goto st_case_12;
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
				}
				p+= 1;
				st_case_1:
				if ( p == pe )
					goto _out1;
				if ( ( (*( p))) < 65 ) {
					if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
						goto _ctr2;
					}
				} else if ( ( (*( p))) > 70 ) {
					if ( 97 <= ( (*( p))) && ( (*( p))) <= 102 ) {
						goto _ctr2;
					}
				} else {
					goto _ctr2;
				}
				goto _st0;
				_st0:
				st_case_0:
				goto _out0;
				_ctr2:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st2;
				_st2:
				p+= 1;
				st_case_2:
				if ( p == pe )
					goto _out2;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr4;
					}
					case 59: {
						goto _ctr5;
					}
				}
				if ( ( (*( p))) < 65 ) {
					if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
						goto _st2;
					}
				} else if ( ( (*( p))) > 70 ) {
					if ( 97 <= ( (*( p))) && ( (*( p))) <= 102 ) {
						goto _st2;
					}
				} else {
					goto _st2;
				}
				goto _st0;
				_ctr4:
				{
					
					_size = str();
				}
				
				
				goto _st3;
				_ctr11:
				{
					
					_name = str();
				}
				
				
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st3;
				_ctr18:
				{
					
					_value = str();
				}
				
				
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st3;
				_ctr27:
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st3;
				_st3:
				p+= 1;
				st_case_3:
				if ( p == pe )
					goto _out3;
				if ( ( (*( p))) == 10 ) {
					goto _ctr7;
				}
				goto _st0;
				_ctr7:
				{
					
					done = true;
					{p+= 1; _fsm_cs = 12; goto _out;}
				}
				
				
				goto _st12;
				_st12:
				p+= 1;
				st_case_12:
				if ( p == pe )
					goto _out12;
				goto _st0;
				_ctr5:
				{
					
					_size = str();
				}
				
				
				goto _st4;
				_ctr12:
				{
					
					_name = str();
				}
				
				
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st4;
				_ctr19:
				{
					
					_value = str();
				}
				
				
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st4;
				_ctr28:
				{
					
					_extensions[_name] = std::move(_value);
				}
				
				
				goto _st4;
				_st4:
				p+= 1;
				st_case_4:
				if ( p == pe )
					goto _out4;
				switch( ( (*( p))) ) {
					case 33: {
						goto _ctr9;
					}
					case 124: {
						goto _ctr9;
					}
					case 126: {
						goto _ctr9;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr9;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr9;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr9;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr9;
						}
					} else {
						goto _ctr9;
					}
				} else {
					goto _ctr9;
				}
				goto _st0;
				_ctr9:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st5;
				_st5:
				p+= 1;
				st_case_5:
				if ( p == pe )
					goto _out5;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr11;
					}
					case 33: {
						goto _st5;
					}
					case 59: {
						goto _ctr12;
					}
					case 61: {
						goto _ctr13;
					}
					case 124: {
						goto _st5;
					}
					case 126: {
						goto _st5;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _st5;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _st5;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _st5;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _st5;
						}
					} else {
						goto _st5;
					}
				} else {
					goto _st5;
				}
				goto _st0;
				_ctr13:
				{
					
					_name = str();
				}
				
				
				goto _st6;
				_st6:
				p+= 1;
				st_case_6:
				if ( p == pe )
					goto _out6;
				switch( ( (*( p))) ) {
					case 34: {
						goto _st8;
					}
					case 124: {
						goto _ctr15;
					}
					case 126: {
						goto _ctr15;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr15;
						}
					} else if ( ( (*( p))) >= 33 ) {
						goto _ctr15;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr15;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr15;
						}
					} else {
						goto _ctr15;
					}
				} else {
					goto _ctr15;
				}
				goto _st0;
				_ctr15:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st7;
				_st7:
				p+= 1;
				st_case_7:
				if ( p == pe )
					goto _out7;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr18;
					}
					case 33: {
						goto _st7;
					}
					case 59: {
						goto _ctr19;
					}
					case 124: {
						goto _st7;
					}
					case 126: {
						goto _st7;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _st7;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _st7;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _st7;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _st7;
						}
					} else {
						goto _st7;
					}
				} else {
					goto _st7;
				}
				goto _st0;
				_st8:
				p+= 1;
				st_case_8:
				if ( p == pe )
					goto _out8;
				switch( ( (*( p))) ) {
					case 34: {
						goto _ctr21;
					}
					case 92: {
						goto _ctr22;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr20;
				_ctr20:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st9;
				_st9:
				p+= 1;
				st_case_9:
				if ( p == pe )
					goto _out9;
				switch( ( (*( p))) ) {
					case 34: {
						goto _ctr24;
					}
					case 92: {
						goto _ctr25;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _st9;
				_ctr21:
				{
					
					g.mark_start(p);
				}
				
				
				{
					
					_value = str();
				}
				
				
				goto _st10;
				_ctr24:
				{
					
					_value = str();
				}
				
				
				goto _st10;
				_st10:
				p+= 1;
				st_case_10:
				if ( p == pe )
					goto _out10;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr27;
					}
					case 59: {
						goto _ctr28;
					}
				}
				goto _st0;
				_ctr22:
				{
					
					g.mark_start(p);
				}
				
				
				{
					g.mark_end(p); }
				
				
				goto _st11;
				_ctr25:
				{
					g.mark_end(p); }
				
				
				goto _st11;
				_st11:
				p+= 1;
				st_case_11:
				if ( p == pe )
					goto _out11;
				if ( ( (*( p))) == 127 ) {
					goto _st0;
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr20;
				_out1: _fsm_cs = 1; goto _out; 
				_out0: _fsm_cs = 0; goto _out; 
				_out2: _fsm_cs = 2; goto _out; 
				_out3: _fsm_cs = 3; goto _out; 
				_out12: _fsm_cs = 12; goto _out; 
				_out4: _fsm_cs = 4; goto _out; 
				_out5: _fsm_cs = 5; goto _out; 
				_out6: _fsm_cs = 6; goto _out; 
				_out7: _fsm_cs = 7; goto _out; 
				_out8: _fsm_cs = 8; goto _out; 
				_out9: _fsm_cs = 9; goto _out; 
				_out10: _fsm_cs = 10; goto _out; 
				_out11: _fsm_cs = 11; goto _out; 
				_out: {}
			}
			
			
#pragma GCC diagnostic pop
#ifdef __clang__
#pragma clang diagnostic pop
#endif
			if (!done) {
				if (p == eof) {
					_state = state::eof;
				} else if (p != pe) {
					_state = state::error;
				} else {
					p = nullptr;
				}
			} else {
				_state = state::done;
			}
			return p;
		}
		auto get_parsed_extensions() {
			return std::move(_extensions);
		}
		auto get_size() {
			return std::move(_size);
		}
		bool eof() const {
			return _state == state::eof;
		}
		bool failed() const {
			return _state == state::error;
		}
	};
	
	
	
	// the headers in the chunk trailer are parsed the same way as in the request_parser.rl
	
	
	
	class http_chunk_trailer_parser : public ragel_parser_base<http_chunk_trailer_parser> {
		
		static const int start = 1;
		static const int error = 0;
		
		static const int en_main = 1;
		
		
		
		public:
		enum class state {
			error,
			eof,
			done,
		};
		std::unordered_map<sstring, sstring> _headers;
		sstring _field_name;
		sstring _value;
		state _state;
		public:
		void init() {
			init_base();
			_headers = std::unordered_map<sstring, sstring>();
			_field_name = "";
			_value = "";
			_state = state::eof;
			
			{
				_fsm_cs = (int)start;
			}
			
			
		}
		char* parse(char* p, char* pe, char* eof) {
			sstring_builder::guard g(_builder, p, pe);
			[[maybe_unused]] auto str = [this, &g, &p] { g.mark_end(p); return get_str(); };
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
					case 12:
					goto st_case_12;
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
				}
				p+= 1;
				st_case_1:
				if ( p == pe )
					goto _out1;
				switch( ( (*( p))) ) {
					case 13: {
						goto _st2;
					}
					case 33: {
						goto _ctr3;
					}
					case 124: {
						goto _ctr3;
					}
					case 126: {
						goto _ctr3;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr3;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr3;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr3;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr3;
						}
					} else {
						goto _ctr3;
					}
				} else {
					goto _ctr3;
				}
				goto _st0;
				_st0:
				st_case_0:
				goto _out0;
				_ctr15:
				{
					
					if (_headers.count(_field_name)) {
						_headers[_field_name] += sstring(",") + std::move(_value);
					} else {
						_headers[_field_name] = std::move(_value);
					}
				}
				
				
				goto _st2;
				_ctr25:
				{
					
					_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				goto _st2;
				_st2:
				p+= 1;
				st_case_2:
				if ( p == pe )
					goto _out2;
				if ( ( (*( p))) == 10 ) {
					goto _ctr4;
				}
				goto _st0;
				_ctr4:
				{
					
					done = true;
					{p+= 1; _fsm_cs = 12; goto _out;}
				}
				
				
				goto _st12;
				_st12:
				p+= 1;
				st_case_12:
				if ( p == pe )
					goto _out12;
				goto _st0;
				_ctr3:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st3;
				_ctr16:
				{
					
					if (_headers.count(_field_name)) {
						_headers[_field_name] += sstring(",") + std::move(_value);
					} else {
						_headers[_field_name] = std::move(_value);
					}
				}
				
				
				{
					
					g.mark_start(p);
				}
				
				
				goto _st3;
				_ctr26:
				{
					
					_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				{
					
					g.mark_start(p);
				}
				
				
				goto _st3;
				_st3:
				p+= 1;
				st_case_3:
				if ( p == pe )
					goto _out3;
				switch( ( (*( p))) ) {
					case 33: {
						goto _st3;
					}
					case 58: {
						goto _ctr6;
					}
					case 124: {
						goto _st3;
					}
					case 126: {
						goto _st3;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _st3;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _st3;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _st3;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _st3;
						}
					} else {
						goto _st3;
					}
				} else {
					goto _st3;
				}
				goto _st0;
				_ctr6:
				{
					
					_field_name = str();
				}
				
				
				goto _st4;
				_st4:
				p+= 1;
				st_case_4:
				if ( p == pe )
					goto _out4;
				switch( ( (*( p))) ) {
					case 9: {
						goto _st4;
					}
					case 13: {
						goto _ctr9;
					}
					case 32: {
						goto _st4;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr8;
				_ctr8:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st5;
				_st5:
				p+= 1;
				st_case_5:
				if ( p == pe )
					goto _out5;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr11;
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
				goto _st5;
				_ctr9:
				{
					
					g.mark_start(p);
				}
				
				
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st6;
				_ctr11:
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st6;
				_st6:
				p+= 1;
				st_case_6:
				if ( p == pe )
					goto _out6;
				if ( ( (*( p))) == 10 ) {
					goto _st7;
				}
				goto _st0;
				_st7:
				p+= 1;
				st_case_7:
				if ( p == pe )
					goto _out7;
				switch( ( (*( p))) ) {
					case 9: {
						goto _ctr14;
					}
					case 13: {
						goto _ctr15;
					}
					case 32: {
						goto _ctr14;
					}
					case 33: {
						goto _ctr16;
					}
					case 124: {
						goto _ctr16;
					}
					case 126: {
						goto _ctr16;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr16;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr16;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr16;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr16;
						}
					} else {
						goto _ctr16;
					}
				} else {
					goto _ctr16;
				}
				goto _st0;
				_ctr14:
				{
					
					if (_headers.count(_field_name)) {
						_headers[_field_name] += sstring(",") + std::move(_value);
					} else {
						_headers[_field_name] = std::move(_value);
					}
				}
				
				
				goto _st8;
				_ctr24:
				{
					
					_headers[_field_name] += sstring(" ") + std::move(_value);
				}
				
				
				goto _st8;
				_st8:
				p+= 1;
				st_case_8:
				if ( p == pe )
					goto _out8;
				switch( ( (*( p))) ) {
					case 9: {
						goto _st8;
					}
					case 13: {
						goto _ctr19;
					}
					case 32: {
						goto _st8;
					}
					case 127: {
						goto _st0;
					}
				}
				if ( 0 <= ( (*( p))) && ( (*( p))) <= 31 ) {
					goto _st0;
				}
				goto _ctr18;
				_ctr18:
				{
					
					g.mark_start(p);
				}
				
				
				goto _st9;
				_st9:
				p+= 1;
				st_case_9:
				if ( p == pe )
					goto _out9;
				switch( ( (*( p))) ) {
					case 13: {
						goto _ctr21;
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
				goto _st9;
				_ctr19:
				{
					
					g.mark_start(p);
				}
				
				
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st10;
				_ctr21:
				{
					
					_value = str();
					trim_trailing_spaces_and_tabs(_value);
					g.mark_start(nullptr);
				}
				
				
				goto _st10;
				_st10:
				p+= 1;
				st_case_10:
				if ( p == pe )
					goto _out10;
				if ( ( (*( p))) == 10 ) {
					goto _st11;
				}
				goto _st0;
				_st11:
				p+= 1;
				st_case_11:
				if ( p == pe )
					goto _out11;
				switch( ( (*( p))) ) {
					case 9: {
						goto _ctr24;
					}
					case 13: {
						goto _ctr25;
					}
					case 32: {
						goto _ctr24;
					}
					case 33: {
						goto _ctr26;
					}
					case 124: {
						goto _ctr26;
					}
					case 126: {
						goto _ctr26;
					}
				}
				if ( ( (*( p))) < 45 ) {
					if ( ( (*( p))) > 39 ) {
						if ( 42 <= ( (*( p))) && ( (*( p))) <= 43 ) {
							goto _ctr26;
						}
					} else if ( ( (*( p))) >= 35 ) {
						goto _ctr26;
					}
				} else if ( ( (*( p))) > 46 ) {
					if ( ( (*( p))) < 65 ) {
						if ( 48 <= ( (*( p))) && ( (*( p))) <= 57 ) {
							goto _ctr26;
						}
					} else if ( ( (*( p))) > 90 ) {
						if ( 94 <= ( (*( p))) && ( (*( p))) <= 122 ) {
							goto _ctr26;
						}
					} else {
						goto _ctr26;
					}
				} else {
					goto _ctr26;
				}
				goto _st0;
				_out1: _fsm_cs = 1; goto _out; 
				_out0: _fsm_cs = 0; goto _out; 
				_out2: _fsm_cs = 2; goto _out; 
				_out12: _fsm_cs = 12; goto _out; 
				_out3: _fsm_cs = 3; goto _out; 
				_out4: _fsm_cs = 4; goto _out; 
				_out5: _fsm_cs = 5; goto _out; 
				_out6: _fsm_cs = 6; goto _out; 
				_out7: _fsm_cs = 7; goto _out; 
				_out8: _fsm_cs = 8; goto _out; 
				_out9: _fsm_cs = 9; goto _out; 
				_out10: _fsm_cs = 10; goto _out; 
				_out11: _fsm_cs = 11; goto _out; 
				_out: {}
			}
			
			
#pragma GCC diagnostic pop
#ifdef __clang__
#pragma clang diagnostic pop
#endif
			if (!done) {
				if (p == eof) {
					_state = state::eof;
				} else if (p != pe) {
					_state = state::error;
				} else {
					p = nullptr;
				}
			} else {
				_state = state::done;
			}
			return p;
		}
		auto get_parsed_headers() {
			return std::move(_headers);
		}
		bool eof() const {
			return _state == state::eof;
		}
		bool failed() const {
			return _state == state::error;
		}
	};
	
}
