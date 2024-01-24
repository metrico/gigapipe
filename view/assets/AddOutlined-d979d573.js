import{i as t,d as i,j as g,D as u}from"./index-cb8d8b59.js";import{B as f}from"./DeleteOutlineOutlined-2068af5e.js";import{r as m,i as h}from"./createSvgIcon-b45af2a3.js";import{j as b}from"./reactDnd-72acd3f4.js";const T=t("div",{target:"e1m1j6jk11"})("overflow-x:hidden;border-radius:3px;background:",({theme:e})=>e.background,";color:",({theme:e})=>e.contrast,";width:100%;height:100%;overflow-y:auto;display:flex;flex:1;flex-direction:column;height:100%;align-items:center;.cont{max-width:1440px;padding:10px;margin:10px;width:100%;background:",({theme:e})=>e.shadow,";display:flex;flex-direction:column;flex:1;overflow-x:hidden;}.ds-header{padding:10px;padding-bottom:20px;font-size:24px;display:flex;margin:10px;justify-content:space-between;align-items:center;padding-left:0px;.logo{margin-right:10px;}}.ds-cont{margin-bottom:10px;border:1px solid ",({theme:e})=>e.accentNeutral,";border-radius:3px;color:",({theme:e})=>e.contrast,";}.ds-item{padding:10px;border-radius:3px 3px 0px 0px;padding-bottom:14px;display:flex;color:",({theme:e})=>e.contrast,";.logo{padding:10px;padding-right:20px;padding-left:0px;}.ds-text{display:flex;flex-direction:column;flex:1;}.ds-type{font-size:18px;padding:10px;padding-left:0px;color:",({theme:e})=>e.contrast,";}small{font-size:12px;}.setting-icon{justify-self:flex-end;cursor:pointer;}.ds-settings{background:",({theme:e})=>e.background,";}}.plugins-cont{display:flex;flex:1;margin:0px 10px;flex-direction:column;padding:10px 20px;border:1px solid ",({theme:e})=>e.accentNeutral,";border-radius:3px;height:fit-content;.title{font-size:14px;padding:10px 0px;}}"),y=t("div",{target:"e1m1j6jk10"})("color:",({theme:e})=>e.contrast,";display:flex;align-items:center;font-size:12px;padding:0px 10px;white-space:nowrap;",e=>e.width!==null?`width:${e.width}px;`:""," border-radius:3px 0px 0px 3px;display:flex;align-items:center;height:28px;"),j=t("input",{target:"e1m1j6jk9"})("display:flex;flex:1;background:",({theme:e})=>e.deep,";color:",({theme:e})=>e.contrast,";border:1px solid ",e=>e.error?"#b62c14":e.theme.accentNeutral,";border-radius:3px;justify-self:flex-end;height:26px;padding-left:8px;"),E=t("textarea",{target:"e1m1j6jk8"})("display:flex;flex:1;background:",({theme:e})=>e.deep,";color:",({theme:e})=>e.contrast,";border:1px solid ",({theme:e})=>e.accentNeutral,";border-radius:3px;justify-self:flex-end;padding-left:8px;"),w=t("div",{target:"e1m1j6jk7"})("display:flex;flex-direction:row;margin-top:5px;align-items:center;",e=>e!=null&&e.width&&(e==null?void 0:e.width)==="normal"?"":"flex:1;","select{background:",({theme:e})=>e.deep,";color:",({theme:e})=>e.contrast,";border:1px solid ",({theme:e})=>e.accentNeutral,";border-radius:3px;font-size:12px;height:30px;display:flex;align-items:center;padding:1px 2px 1px 8px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;option{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}"),L=t("div",{target:"e1m1j6jk6"})({name:"1uek1ww",styles:"display:flex;margin:15px 0px;margin-left:14px;flex-wrap:wrap;align-items:center;flex:1;&.internal{max-width:400px;}"}),P=t("div",{target:"e1m1j6jk4"})({name:"e23o9g",styles:"padding:10px"}),q=t("div",{target:"e1m1j6jk3"})("margin:10px 0px;padding-bottom:10px;border-bottom:1px solid ",({theme:e})=>e.background,";"),B=t("div",{target:"e1m1j6jk2"})("padding:10px;border-bottom:1px solid ",({theme:e})=>e.shadow,";border-radius:3px;display:flex;flex:1;align-items:center;justify-content:space-between;.edit-buttons{display:flex;align-items:center;&:disabled{display:none;}}"),F=t("div",{target:"e1m1j6jk1"})("background:",({theme:e})=>e.deep,";padding:10px;border-radius:0px 0px 3px 3px;border-top:1px solid ",({theme:e})=>e.accentNeutral,";"),M=t(f,{target:"e1m1j6jk0"})("background:",e=>e.primary?e.theme.primary:e.theme.neutral,";border:1px solid ",({theme:e})=>e.accentNeutral,";color:",e=>e.primary?e.theme.maxContrast:e.theme.contrast,";margin-left:5px;transition:0.25s all;justify-content:center;padding:3px 12px;height:28px;display:flex;&:hover{background:",({theme:e})=>e.primaryLight,";color:",e=>e.primary?e.theme.contrast:e.theme.maxContrast,";}&:disabled{background:",({theme:e})=>e.neutral,";border:1px solid ",({theme:e})=>e.accentNeutral,";cursor:not-allowed;color:",({theme:e})=>e.contrast,";}@media screen and (max-width: 1070px){display:flex;margin:0;}"),v="/assets/metrics_icon-9cba0731.png",n="/assets/logs_icon-972f9506.png",k="/assets/traces_icon-a9ed0318.png",_={metrics_icon:v,logs_icon:n,traces_icon:k},G=({icon:e,style:a})=>i("img",{height:"40px",className:"logo",style:a,src:_[e]||n,alt:e}),H=e=>{const{value:a,label:d,onChange:o,locked:l,type:s,placeholder:p,error:x,labelWidth:c}=e;return g(w,{children:[i(y,{width:c||null,children:d}),i(j,{className:"ds-input",disabled:l,error:x||!1,onChange:o,type:s,value:u.sanitize(a),placeholder:p})]})},U=e=>a=>{a({type:"SET_DATA_SOURCES",dataSources:e})};var r={},S=h;Object.defineProperty(r,"__esModule",{value:!0});var N=r.default=void 0,C=S(m()),D=b,I=(0,C.default)((0,D.jsx)("path",{d:"M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"}),"AddOutlined");N=r.default=I;export{M as D,H as F,w as I,y as L,T as P,B as S,E as T,L as a,q as b,P as c,N as d,G as e,F as f,U as s};
