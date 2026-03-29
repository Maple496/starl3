
import os
import re
import json
import time
import random
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from core.constants import BASE_DIR
from core.pipeline_engine import PipelineEngine
from core.registry import op
from core.context import PipelineContext

# 扩展 User-Agent 列表
_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

_HEADER_TEMPLATES = [
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    },
]


def _get_session(ctx) -> requests.Session:
    """从上下文获取或创建 Session
    
    替代原有的全局 _session 变量，支持多线程/多 Pipeline 并发
    """
    if isinstance(ctx, PipelineContext):
        return ctx.session
    
    # 兼容旧的字典格式
    if "_session" not in ctx:
        ctx["_session"] = requests.Session()
    return ctx["_session"]


def _clear_session(ctx):
    """清除上下文中的 Session"""
    if isinstance(ctx, PipelineContext):
        ctx.clear_session()
    elif "_session" in ctx:
        ctx["_session"] = requests.Session()


def _get_random_ua():
    return random.choice(_USER_AGENTS)


def _get_random_headers():
    """获取随机请求头组合"""
    headers = _HEADER_TEMPLATES[0].copy()
    headers['User-Agent'] = _get_random_ua()
    return headers


def _make_request(ctx, method, url, **kwargs):
    """统一请求方法，包含重试逻辑
    
    Args:
        ctx: 上下文对象
        method: HTTP 方法
        url: 请求 URL
        **kwargs: 其他请求参数
    """
    max_retries = kwargs.pop('max_retries', 3)
    retry_delay = kwargs.pop('retry_delay', 2)
    use_random_delay = kwargs.pop('use_random_delay', True)
    
    session = _get_session(ctx)
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            if use_random_delay and attempt > 0:
                delay = retry_delay * (attempt + 1) + random.uniform(0.5, 2)
                time.sleep(delay)
            
            response = session.request(method, url, **kwargs)
            
            if response.status_code == 403:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * 2)
                    continue
            elif response.status_code == 429:
                wait_time = int(response.headers.get('Retry-After', retry_delay * 5))
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    continue
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            continue
    
    if last_exception:
        raise last_exception
    return None


@op("http_request", category="crawler", description="发送 HTTP 请求")
def op_http_request(ctx, params):
    """发送 HTTP 请求 - 增强版"""
    method = params.get('method', 'GET').upper()
    url = params.get('url', '')
    
    if not url:
        return {'status': 'error', 'message': 'URL 不能为空'}
    
    headers = params.get('headers', {})
    
    if params.get('use_full_headers', True):
        base_headers = _get_random_headers()
        base_headers.update(headers)
        headers = base_headers
    elif params.get('random_ua', True) and 'User-Agent' not in headers:
        headers['User-Agent'] = _get_random_ua()
    
    if params.get('auto_referer', True) and 'Referer' not in headers:
        parsed = urlparse(url)
        referer = f"{parsed.scheme}://{parsed.netloc}/"
        headers['Referer'] = referer
    
    request_kwargs = {
        'headers': headers,
        'timeout': params.get('timeout', 30),
        'max_retries': params.get('max_retries', 3),
        'retry_delay': params.get('retry_delay', 2),
        'use_random_delay': params.get('use_random_delay', True),
    }
    
    if params.get('cookies'):
        request_kwargs['cookies'] = params['cookies']
    if params.get('proxy'):
        request_kwargs['proxies'] = {'http': params['proxy'], 'https': params['proxy']}
    if params.get('data'):
        request_kwargs['data'] = params['data']
    if params.get('json_data'):
        request_kwargs['json'] = params['json_data']
    if params.get('params'):
        request_kwargs['params'] = params['params']
    
    try:
        response = _make_request(ctx, method, url, **request_kwargs)
        
        result = {
            'status': 'success',
            'status_code': response.status_code,
            'url': response.url,
            'headers': dict(response.headers),
        }
        
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/json' in content_type:
            try:
                result['data'] = response.json()
                result['type'] = 'json'
            except:
                result['data'] = response.text
                result['type'] = 'text'
        elif 'text/html' in content_type:
            result['data'] = response.text
            result['type'] = 'html'
        else:
            result['data'] = response.text
            result['type'] = 'text'
        
        return result
        
    except Exception as e:
        return {'status': 'error', 'message': str(e), 'url': url}


@op("parse_html", category="crawler", description="解析 HTML")
def op_parse_html(ctx, params):
    """解析 HTML 提取数据"""
    html = params.get('html', '')
    
    if not html and isinstance(ctx.get('last_result'), dict):
        html = ctx['last_result'].get('data', '')
    if not html and isinstance(ctx.get('last_result'), str):
        html = ctx['last_result']
    
    if not html:
        return {'status': 'error', 'message': '没有 HTML 内容可解析'}
    
    soup = BeautifulSoup(html, 'html.parser')
    
    rules = params.get('rules', [])
    if isinstance(rules, dict):
        rules = [{'name': k, 'selector': v, 'type': 'css'} for k, v in rules.items()]
    
    results = {}
    
    for rule in rules:
        name = rule.get('name', 'data')
        selector = rule.get('selector', '')
        selector_type = rule.get('type', 'css')
        extract_type = rule.get('extract', 'text')
        attr_name = rule.get('attr', '')
        multiple = rule.get('multiple', True)
        
        if not selector:
            continue
        
        items = []
        
        if selector_type == 'css':
            elements = soup.select(selector)
        elif selector_type == 'regex':
            matches = re.findall(selector, str(soup))
            items = matches if multiple else ([matches[0]] if matches else [])
            results[name] = items if multiple else (items[0] if items else None)
            continue
        else:
            elements = []
        
        for elem in elements:
            if extract_type == 'text':
                items.append(elem.get_text(strip=True))
            elif extract_type == 'html':
                items.append(str(elem))
            elif extract_type == 'attr' and attr_name:
                items.append(elem.get(attr_name, ''))
        
        if multiple:
            results[name] = items
        else:
            results[name] = items[0] if items else None
    
    return {
        'status': 'success',
        'extracted': results,
        'title': soup.title.string if soup.title else '',
        'links': [a.get('href') for a in soup.find_all('a', href=True)],
        'images': [img.get('src') for img in soup.find_all('img', src=True)]
    }


@op("crawl_pages", category="crawler", description="分页爬取")
def op_crawl_pages(ctx, params):
    """分页爬取 - 使用上下文 Session"""
    url_template = params.get('url_template', '')
    page_param = params.get('page_param', 'page')
    start_page = params.get('start_page', 1)
    end_page = params.get('end_page', 1)
    max_pages = params.get('max_pages', 100)
    
    delay = params.get('delay', 3)
    delay_random = params.get('delay_random', True)
    delay_range = params.get('delay_range', [0.5, 2])
    
    proxies = params.get('proxies', [])
    proxy_rotate = params.get('proxy_rotate', False)
    
    resume_from = params.get('resume_from', None)
    save_progress = params.get('save_progress', True)
    progress_file = params.get('progress_file', 'crawler_progress.json')
    
    merge_results = params.get('merge_results', True)
    
    all_results = [] if not merge_results else {}
    page = resume_from if resume_from else start_page
    pages_crawled = 0
    proxy_index = 0
    errors = []
    
    while page <= end_page and pages_crawled < max_pages:
        format_kwargs = {page_param: page}
        if page_param != 'page':
            format_kwargs['page'] = page
        current_url = url_template.format(**format_kwargs)
        
        current_proxy = None
        if proxies and proxy_rotate:
            current_proxy = proxies[proxy_index % len(proxies)]
            proxy_index += 1
        elif proxies:
            current_proxy = random.choice(proxies)
        
        request_params = {
            'url': current_url,
            'method': params.get('method', 'GET'),
            'headers': params.get('headers', {}),
            'use_full_headers': params.get('use_full_headers', True),
            'auto_referer': params.get('auto_referer', True),
            'timeout': params.get('timeout', 30),
            'max_retries': params.get('max_retries', 3),
            'retry_delay': params.get('retry_delay', 2),
            'use_random_delay': False,
        }
        
        if current_proxy:
            request_params['proxy'] = current_proxy
        
        try:
            result = op_http_request(ctx, request_params)
            
            if result.get('status') == 'success':
                page_data = {
                    'page': page,
                    'url': current_url,
                    'status': 'success',
                    'data': result.get('data'),
                    'type': result.get('type')
                }
                
                parsed_data = None
                if params.get('parse_rules'):
                    temp_ctx = {'last_result': result}
                    parsed = op_parse_html(temp_ctx, {'rules': params['parse_rules']})
                    parsed_data = parsed.get('extracted', {})
                    page_data['parsed'] = parsed_data
                    
                    if merge_results and parsed_data:
                        for key, values in parsed_data.items():
                            if isinstance(values, list):
                                if key not in all_results:
                                    all_results[key] = []
                                all_results[key].extend(values)
                            else:
                                if key not in all_results:
                                    all_results[key] = []
                                all_results[key].append(values)
                
                if not merge_results:
                    all_results.append(page_data)
                
                pages_crawled += 1
                
                if save_progress:
                    progress = {
                        'last_page': page,
                        'pages_crawled': pages_crawled,
                        'timestamp': time.time()
                    }
                    try:
                        with open(progress_file, 'w') as f:
                            json.dump(progress, f)
                    except:
                        pass
            else:
                error_info = {
                    'page': page,
                    'url': current_url,
                    'status': 'error',
                    'message': result.get('message')
                }
                errors.append(error_info)
                if not merge_results:
                    all_results.append(error_info)
        
        except Exception as e:
            error_info = {
                'page': page,
                'url': current_url,
                'status': 'error',
                'message': str(e)
            }
            errors.append(error_info)
            if not merge_results:
                all_results.append(error_info)
        
        page += 1
        
        if page <= end_page:
            actual_delay = delay
            if delay_random:
                actual_delay += random.uniform(delay_range[0], delay_range[1])
            time.sleep(actual_delay)
    
    return {
        'status': 'success',
        'pages_crawled': pages_crawled,
        'errors_count': len(errors),
        'errors': errors if errors else None,
        'merged': merge_results,
        'results': all_results if not merge_results else {'extracted': all_results, 'pages': pages_crawled}
    }


@op("crawl_recursive", category="crawler", description="递归爬取")
def op_crawl_recursive(ctx, params):
    """递归爬取链接 - 使用上下文 Session"""
    start_url = params.get('start_url', '')
    max_depth = params.get('max_depth', 2)
    max_pages = params.get('max_pages', 50)
    same_domain = params.get('same_domain', True)
    allowed_extensions = params.get('allowed_extensions', ['.html', '.htm', '.php', '.jsp', ''])
    delay = params.get('delay', 2)
    delay_random = params.get('delay_random', True)
    
    if not start_url:
        return {'status': 'error', 'message': '起始 URL 不能为空'}
    
    base_domain = urlparse(start_url).netloc
    visited = set()
    to_visit = [(start_url, 0)]
    results = []
    
    while to_visit and len(results) < max_pages:
        url, depth = to_visit.pop(0)
        
        if url in visited or depth > max_depth:
            continue
        
        visited.add(url)
        
        result = op_http_request(ctx, {
            'url': url,
            'method': 'GET',
            'use_full_headers': True,
            'auto_referer': True,
            'timeout': 30
        })
        
        if result.get('status') == 'success':
            page_data = {
                'url': url,
                'depth': depth,
                'type': result.get('type'),
                'data': result.get('data') if params.get('save_content') else None
            }
            
            if depth < max_depth:
                soup = BeautifulSoup(result.get('data', ''), 'html.parser')
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    full_url = urljoin(url, href)
                    parsed = urlparse(full_url)
                    
                    if same_domain and parsed.netloc != base_domain:
                        continue
                    
                    ext = Path(parsed.path).suffix.lower()
                    if ext not in allowed_extensions:
                        continue
                    
                    if full_url not in visited:
                        to_visit.append((full_url, depth + 1))
            
            results.append(page_data)
        else:
            results.append({
                'url': url,
                'depth': depth,
                'status': 'error',
                'message': result.get('message')
            })
        
        actual_delay = delay
        if delay_random:
            actual_delay += random.uniform(0.5, 2)
        time.sleep(actual_delay)
    
    return {
        'status': 'success',
        'pages_crawled': len(results),
        'visited_urls': list(visited),
        'results': results
    }


@op("session_login", category="crawler", description="登录并保持会话")
def op_session_login(ctx, params):
    """登录并保持会话 - 使用上下文 Session"""
    login_url = params.get('login_url', '')
    username_field = params.get('username_field', 'username')
    password_field = params.get('password_field', 'password')
    username = params.get('username', '')
    password = params.get('password', '')
    
    login_data = params.get('data', {})
    login_data[username_field] = username
    login_data[password_field] = password
    
    for key, value in params.get('extra_fields', {}).items():
        login_data[key] = value
    
    try:
        headers = _get_random_headers()
        session = _get_session(ctx)
        response = session.post(
            login_url,
            data=login_data,
            headers=headers,
            timeout=params.get('timeout', 30)
        )
        response.raise_for_status()
        
        success_check = params.get('success_check', '')
        if success_check:
            success = success_check in response.text
        else:
            success = response.status_code == 200
        
        return {
            'status': 'success' if success else 'failed',
            'status_code': response.status_code,
            'cookies': dict(session.cookies),
            'response_preview': response.text[:500] if params.get('debug') else None
        }
        
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


@op("download_file", category="crawler", description="下载文件")
def op_download_file(ctx, params):
    """下载文件 - 使用上下文 Session"""
    url = params.get('url', '')
    output_path = params.get('output_path', '')
    
    if not url:
        return {'status': 'error', 'message': 'URL 不能为空'}
    
    if not output_path:
        filename = os.path.basename(urlparse(url).path) or 'download'
        output_path = os.path.join('downloads', filename)
    
    full_path = os.path.join(ctx.get('base_dir', BASE_DIR), output_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    try:
        headers = _get_random_headers()
        session = _get_session(ctx)
        response = session.get(
            url,
            headers=headers,
            timeout=params.get('timeout', 60),
            stream=True
        )
        response.raise_for_status()
        
        downloaded = 0
        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        
        return {
            'status': 'success',
            'url': url,
            'saved_path': output_path,
            'full_path': full_path,
            'size': downloaded
        }
        
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


@op("save_crawler_data", category="crawler", description="保存爬虫数据")
def op_save_crawler_data(ctx, params):
    """保存爬虫数据"""
    data = ctx.get('last_result', {})
    
    if isinstance(data, dict):
        if 'extracted' in data:
            data = data['extracted']
        elif 'results' in data:
            data = data['results']
    
    output_format = params.get('format', 'json')
    output_file = params.get('file', 'output/crawler_data')
    
    full_path = os.path.join(ctx.get('base_dir', BASE_DIR), output_file)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    try:
        if output_format == 'json':
            if not full_path.endswith('.json'):
                full_path += '.json'
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
        elif output_format == 'csv':
            import pandas as pd
            if not full_path.endswith('.csv'):
                full_path += '.csv'
            
            if isinstance(data, dict):
                max_len = max([len(v) if isinstance(v, list) else 1 for v in data.values()], default=0)
                df_data = {}
                for key, values in data.items():
                    if isinstance(values, list):
                        df_data[key] = values + [''] * (max_len - len(values))
                    else:
                        df_data[key] = [values] * max_len
                df = pd.DataFrame(df_data)
            elif isinstance(data, list) and len(data) > 0:
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame(data)
            
            df.to_csv(full_path, index=False, encoding='utf-8-sig')
            
        elif output_format == 'excel':
            import pandas as pd
            if not full_path.endswith('.xlsx'):
                full_path += '.xlsx'
            
            if isinstance(data, dict):
                max_len = max([len(v) if isinstance(v, list) else 1 for v in data.values()], default=0)
                df_data = {}
                for key, values in data.items():
                    if isinstance(values, list):
                        df_data[key] = values + [''] * (max_len - len(values))
                    else:
                        df_data[key] = [values] * max_len
                df = pd.DataFrame(df_data)
            elif isinstance(data, list) and len(data) > 0:
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame(data)
            
            df.to_excel(full_path, index=False)
        
        return {
            'status': 'success',
            'format': output_format,
            'file': full_path,
            'records': len(data) if hasattr(data, '__len__') else 1
        }
        
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


@op("clear_session", category="crawler", description="清除会话")
def op_clear_session(ctx, params):
    """清除会话"""
    _clear_session(ctx)
    return {'status': 'cleared'}


@op("set_proxy_pool", category="crawler", description="设置代理池")
def op_set_proxy_pool(ctx, params):
    """设置代理池"""
    proxies = params.get('proxies', [])
    return {
        'status': 'success',
        'proxies_count': len(proxies),
        'proxies': proxies[:3] if len(proxies) > 3 else proxies
    }


def run(config_path=None):
    """模块测试入口"""
    from core.registry import OpRegistry
    PipelineEngine.main(OpRegistry.get_op_map(), cfg=config_path, init_ctx=lambda: {"base_dir": BASE_DIR})


if __name__ == '__main__':
    run()
