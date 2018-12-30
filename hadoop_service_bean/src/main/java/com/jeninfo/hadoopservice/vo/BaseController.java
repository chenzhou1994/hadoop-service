package com.jeninfo.hadoopservice.vo;

/**
 * @author chenzhou
 * @date 2018/12/30 15:55
 * @description
 */
public class BaseController {
    private final static Boolean SUCCESS_SUCCESS = true;

    private final static Boolean SUCCESS_ERROR = false;

    /**
     * 返回成功
     *
     * @param data
     * @param code
     * @param message
     * @return
     */
    public Render renderSuccess(Object data, Integer code, String message) {
        return new Render().setCode(code)
                .setMessage(message)
                .setData(data)
                .setSuccess(SUCCESS_SUCCESS);
    }

    /**
     * 默认返回成功
     *
     * @param code
     * @param message
     * @return
     */
    public Render renderSuccess(Integer code, String message) {

        return new Render().setCode(code)
                .setMessage(message)
                .setSuccess(SUCCESS_SUCCESS);
    }

    /**
     * 返回成功
     *
     * @param code
     * @return
     */
    public Render renderSuccess(Integer code) {
        return new Render().setCode(code)
                .setSuccess(SUCCESS_SUCCESS);
    }

    /**
     * 返回失败
     *
     * @param data
     * @param code
     * @param message
     * @return
     */
    public Render renderError(Object data, Integer code, String message) {
        return new Render().setCode(code)
                .setMessage(message)
                .setData(data)
                .setSuccess(SUCCESS_ERROR);
    }

    /**
     * 返回失败
     *
     * @param code
     * @param message
     * @return
     */
    public Render renderError(Integer code, String message) {
        return new Render().setCode(code)
                .setMessage(message)
                .setSuccess(SUCCESS_ERROR);
    }

    /**
     * 返回失败
     *
     * @param message
     * @return
     */
    public Render renderError(String message) {
        return new Render()
                .setMessage(message)
                .setSuccess(SUCCESS_ERROR);
    }

    /**
     * 默认返回失败
     *
     * @param code
     * @return
     */
    public Render renderError(Integer code) {
        return new Render().setCode(code)
                .setSuccess(SUCCESS_ERROR);
    }

    /**
     * 返回成功 分页信息
     *
     * @param pageData
     * @param page
     * @param pageSize
     * @param totalCount
     * @param code
     * @param message
     * @return
     */
    public RenderPage renderPage(Object pageData, Integer page, Integer pageSize, Integer totalCount, Integer code, String message) {
        RenderPage renderPage = new RenderPage();
        renderPage.setPage(page);
        renderPage.setPageSize(pageSize);
        renderPage.setTotalCount(totalCount);
        renderPage.setCode(code);
        renderPage.setMessage(message);
        renderPage.setData(pageData);
        renderPage.setSuccess(SUCCESS_SUCCESS);
        return renderPage;
    }

    /**
     * 分页
     *
     * @param pageData
     * @param page
     * @param pageSize
     * @param totalCount
     * @param code
     * @param message
     * @return
     */
    public RenderPage renderPage(Object pageData, long page, long pageSize, long totalCount, Integer code, String message) {

        RenderPage renderPage = new RenderPage();
        renderPage.setPage((int) page);
        renderPage.setPageSize((int) pageSize);
        renderPage.setTotalCount((int) totalCount);
        renderPage.setCode(code);
        renderPage.setMessage(message);
        renderPage.setData(pageData);
        renderPage.setSuccess(SUCCESS_SUCCESS);
        return renderPage;
    }
}
