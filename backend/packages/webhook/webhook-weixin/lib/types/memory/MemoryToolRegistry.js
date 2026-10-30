export class MemoryToolRegistry {
    static bound = new WeakMap();
    /** ctx 为作用域，key 为工具组，install 接收受事务保护的注册函数；成功后记录，卸载时移除。 */
    static register_tools(ctx, key, install) {
        const groups = this.bound.get(ctx) ?? new Set();
        if (groups.has(key))
            return;
        const releases = [];
        try {
            install(tool => { releases.push(ctx.tools.register(tool)); });
        }
        catch (error) {
            for (const release of releases.reverse())
                release();
            throw error;
        }
        groups.add(key);
        this.bound.set(ctx, groups);
        ctx.effect(() => () => {
            for (const release of releases.reverse())
                release();
            groups.delete(key);
        }, `shared-memory.${key}`);
    }
}
//# sourceMappingURL=MemoryToolRegistry.js.map